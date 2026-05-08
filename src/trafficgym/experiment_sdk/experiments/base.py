from abc import ABC, abstractmethod
from trafficgym.engine.ports.simulation import SimulationPort

class Experiment(ABC):
    def __init__(self) -> None:
        self._subscriptions: list[tuple[str, str, str, str | None]] = []

    def subscribe(self, name: str, domain: str, getter: str, object_id: str | None) -> None:
        self._subscriptions.append((name, domain, getter, object_id))

    def poll(self, adapter: SimulationPort) -> dict[str, str]:
        return {
            name: adapter.query(domain, getter, object_id, {})
            for name, domain, getter, object_id in self._subscriptions
        }

    @abstractmethod
    def run(self, adapter: SimulationPort) -> None: ...
