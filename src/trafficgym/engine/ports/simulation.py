from __future__ import annotations
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Generator, Mapping, Protocol, Union
import uuid


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
    """Abstract port that all simulation adapters must implement.

    Provides step-driven simulation control (``run_time``, ``run_steps``,
    ``run_until_empty``), controller wiring via :meth:`controlled`, and
    raw query/apply access to SUMO domains.
    """

    def __init__(self, step_length_ms: int) -> None:
        self.run_id = str(uuid.uuid4())
        self.started: bool = False
        self.closed: bool = False
        self.step: int = 0
        self.last_metrics: Observation = {}
        self.max_steps: int | None = None
        self._step_length_ms: int = step_length_ms
        self._controllers: list[_WiredController] = []
        self._after_tick: Callable[[int, float, Observation], None] | None = None
        """Optional callback invoked after every simulation step.

        Signature: ``(step: int, sim_time_s: float, metrics: Observation) -> None``.
        Set by the platform to collect subscription logs; experiments should not
        overwrite this value.
        """

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
        """Advance the simulation by a fixed wall-clock duration.

        Args:
            seconds: Simulation time to advance in seconds.
        """
        steps = round(seconds * self.steps_per_second)
        for _ in range(steps):
            step, time, obs = self.tick()
            for c in self._controllers:
                c.on_tick(self, time)
            if self._after_tick is not None:
                self._after_tick(step, time, obs)

    def run_steps(self, n: int) -> None:
        """Advance the simulation by a fixed number of steps.

        Args:
            n: Number of simulation steps to execute.
        """
        for _ in range(n):
            step, time, obs = self.tick()
            for c in self._controllers:
                c.on_tick(self, time)
            if self._after_tick is not None:
                self._after_tick(step, time, obs)

    def run_until_empty(self) -> None:
        """Advance the simulation until no vehicles remain in the network.

        Polls ``sim.remaining_veh`` after each tick and stops when it reaches
        zero. Suitable for demand-bounded scenarios with a fixed vehicle set.
        Controllers are invoked and subscriptions are collected on every tick.
        """
        while True:
            step, time, metrics = self.tick()
            for c in self._controllers:
                c.on_tick(self, time)
            if self._after_tick is not None:
                self._after_tick(step, time, metrics)
            if int(metrics.get("sim.remaining_veh", 1)) <= 0:
                break

    @abstractmethod
    def start(self) -> None:
        """Start the SUMO simulation process. Must be called before any step methods."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Shut down the SUMO simulation process and release resources."""
        ...

    @abstractmethod
    def tick(self) -> tuple[int, float, Observation]:
        """Advance the simulation by one step.

        Returns:
            Tuple of ``(step_index, simulation_time_seconds, metrics)``.
            ``metrics`` always contains ``sim.remaining_veh``.
        """
        ...

    @abstractmethod
    def query(
        self,
        domain: str,
        getter_name: str,
        object_id: str | None,
        args: Params,
    ) -> str:
        """Read a value from a SUMO domain getter.

        Args:
            domain: SUMO domain name (e.g. ``"lane"``, ``"edge"``).
            getter_name: SUMO getter method (e.g. ``"getLastStepOccupancy"``).
            object_id: Object identifier, or ``None`` for network-wide getters.
            args: Additional keyword arguments forwarded to the getter.

        Returns:
            String representation of the queried value.
        """
        ...

    @abstractmethod
    def apply(
        self,
        domain: str,
        setter_name: str,
        object_id: str | None,
        args: Params,
    ) -> None:
        """Write a value via a SUMO domain setter.

        Args:
            domain: SUMO domain name (e.g. ``"trafficlight"``).
            setter_name: SUMO setter method (e.g. ``"setRedYellowGreenState"``).
            object_id: Object identifier, or ``None`` for network-wide setters.
            args: Keyword arguments forwarded to the setter.
        """
        ...


class _ControllerNode(Protocol):
    def step(self, adapter: SimulationPort, inputs: Any) -> Any: ...


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
        result = self._node.step(adapter, inputs)
        if result:
            self._actuate(adapter, result)
