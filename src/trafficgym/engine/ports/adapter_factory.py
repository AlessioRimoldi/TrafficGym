from abc import ABC, abstractmethod
from trafficgym.engine.ports.simulation import SimulationPort, RunConfig


class AdapterFactory(ABC):
    @abstractmethod
    def create(self, config: RunConfig, step_length_ms: int) -> SimulationPort:
        pass
