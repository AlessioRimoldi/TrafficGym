from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping, Protocol, Union, Callable
import uuid

class Controller(Protocol):
    def on_tick(self, adapter: SimulationPort, sim_time: float) -> None: ...

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
        """All subclasses must call super().__init__()"""
        self.run_id = str(uuid.uuid4())
        self.started: bool = False
        self.closed: bool = False
        self.step: int = 0
        self.last_metrics: Observation = {}
        self.max_steps: int | None = None
        self._step_length_ms: int = step_length_ms
        self._controllers: list[Controller] = []
        self.after_tick: Callable[[int, float, Observation], None] | None = None

    @property
    def steps_per_second(self) -> float:
        return 1000 / (self._step_length_ms)

    @property
    def seconds_per_step(self) -> float:
        return self._step_length_ms / 1000

    def register(self, controller: Controller) -> None:
        self._controllers.append(controller)

    def deregister(self, controller: Controller) -> None:
        self._controllers.remove(controller)
    
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

    """Start the simulation"""

    @abstractmethod
    def close(self) -> None: ...

    """Close the simulation"""

    @abstractmethod
    def tick(self) -> tuple[int, float, Observation]: ...

    """Step the simulation forward.
    Returns the step count, time and
    telemetry after the step"""

    @abstractmethod
    def query(
        self,
        domain: str,
        getter_name: str,
        object_id: str | None,
        args: Params,
    ) -> str: ...

    """query simulation state"""

    @abstractmethod
    def apply(
        self,
        domain: str,
        setter_name: str,
        object_id: str | None,
        args: Params,
    ) -> None: ...

    """set simulation state"""

    # @abstractmethod
    # def list_domains(self) -> list[str]: ...
    # """retrieve a list of supported domain names of this adapter"""

    # @abstractmethod
    # def list_methods(self, domain: str) -> list[str]: ...
    # """retrieve a list of support method within a domain of this adapter"""
