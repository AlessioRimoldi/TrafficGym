from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from trafficgym.api import engine_pb2
from typing import Any
import asyncio
import uuid
import uuid

ValDict = dict[str, engine_pb2.CustomValue]


@dataclass
class RunConfig:
    sumocfg_path: str
    sumo_binary: str


@dataclass
class InterruptEvent:
    event_id = str(uuid.uuid4())
    observed_value: engine_pb2.CustomValue


@dataclass
class Interrupt:
    trigger_metric_fingerprint: str
    trigger_metric_value: engine_pb2.CustomValue
    trigger_metric_op: engine_pb2.Operation.ValueType
    interrupt_requests: asyncio.Queue[InterruptEvent | None]
    active_interrupt_event: InterruptEvent | None = None
    interrupt_id: str = field(default_factory=lambda: str(uuid.uuid4()))


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
        self.last_metrics: ValDict = {}
        self.max_steps: int | None = None
        self.interrupts: dict[str, Interrupt] = {}
        self._step_length_ms: int = step_length_ms
        self._initialised = True

    @property
    def steps_per_second(self) -> float:
        return 1000 / (self._step_length_ms)

    @property
    def seconds_per_step(self) -> float:
        return self._step_length_ms / 1000

    @abstractmethod
    def start(self) -> None: ...

    """Start the simulation"""

    @abstractmethod
    def close(self) -> None: ...

    """Close the simulation"""

    @abstractmethod
    def tick(self) -> tuple[int, float, ValDict]: ...

    """Step the simulation forward.
    Returns the step count, time and
    telemetry after the step"""

    @abstractmethod
    def query(
        self,
        domain: str,
        getter_name: str,
        object_id: str | None,
        args: ValDict,
    ) -> str: ...

    """query simulation state"""

    @abstractmethod
    def apply(
        self,
        domain: str,
        setter_name: str,
        onject_id: str | None,
        args: ValDict,
    ) -> None: ...

    """set simulation state"""

    # @abstractmethod
    # def list_domains(self) -> list[str]: ...
    # """retrieve a list of supported domain names of this adapter"""

    # @abstractmethod
    # def list_methods(self, domain: str) -> list[str]: ...
    # """retrieve a list of support method within a domain of this adapter"""
