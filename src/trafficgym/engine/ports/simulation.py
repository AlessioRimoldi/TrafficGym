from __future__ import annotations
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Generator, Mapping, Protocol, Union
import uuid


class _ControllerNode(Protocol):
    def step(self, inputs: Any) -> Any: ...


MappingValue = Union[str, int, float]
Observation = Mapping[str, MappingValue]
Params = Mapping[str, MappingValue]


@dataclass
class RunConfig:
    sumocfg_path: str
    sumo_binary: str
    seed: int


class InvalidGetterError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class SimulationPort(ABC):
    def __init__(self, step_length_ms: int) -> None:
        self.run_id = str(uuid.uuid4())
        self.started: bool = False
        self.closed: bool = False
        self.step: int = 0
        self.last_metrics: Observation = {}
        self.max_steps: int | None = None
        self._step_length_ms: int = step_length_ms
        self._controllers: list[_WiredController] = []
        self.after_tick: Callable[[int, float, Observation], None] | None = None

    @property
    def steps_per_second(self) -> float:
        return 1000 / self._step_length_ms

    @property
    def seconds_per_step(self) -> float:
        return self._step_length_ms / 1000

    @contextmanager
    def controlled(
        self,
        controller: _ControllerNode,
        *,
        observe: Callable[[SimulationPort, float], dict[str, Any]],
        actuate: Callable[[SimulationPort, dict[str, Any]], None],
    ) -> Generator[None, None, None]:
        bound = _WiredController(controller, observe, actuate)
        self._controllers.append(bound)
        try:
            yield
        finally:
            self._controllers.remove(bound)

    def run_time(self, seconds: float) -> None:
        steps = round(seconds * self.steps_per_second)
        for _ in range(steps):
            step, time, obs = self.tick()
            for c in self._controllers:
                c.on_tick(self, time)
            if self.after_tick is not None:
                self.after_tick(step, time, obs)

    def run_steps(self, n: int) -> None:
        for _ in range(n):
            step, time, obs = self.tick()
            for c in self._controllers:
                c.on_tick(self, time)
            if self.after_tick is not None:
                self.after_tick(step, time, obs)

    def run_until_empty(self) -> None:
        while True:
            _, _, metrics = self.tick()
            if metrics.get("sim.remaining_veh", 1) == 0:
                break

    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def tick(self) -> tuple[int, float, Observation]: ...

    @abstractmethod
    def query(
        self,
        domain: str,
        getter_name: str,
        object_id: str | None,
        args: Params,
    ) -> str: ...

    @abstractmethod
    def apply(
        self,
        domain: str,
        setter_name: str,
        object_id: str | None,
        args: Params,
    ) -> None: ...


class _WiredController:
    def __init__(
        self,
        node: _ControllerNode,
        observe: Callable[[SimulationPort, float], dict[str, Any]],
        actuate: Callable[[SimulationPort, dict[str, Any]], None],
    ) -> None:
        self._node = node
        self._observe = observe
        self._actuate = actuate

    def on_tick(self, adapter: SimulationPort, sim_time: float) -> None:
        inputs = self._observe(adapter, sim_time)
        result = self._node.step(inputs)
        if result:
            self._actuate(adapter, result)
