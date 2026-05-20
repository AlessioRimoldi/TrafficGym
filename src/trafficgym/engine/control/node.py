from typing import Protocol, Any
from trafficgym.engine.ports.simulation import SimulationPort


class Block(Protocol):
    def step(self, adapter: SimulationPort, inputs: dict[str, Any]) -> dict[str, Any]: ...
