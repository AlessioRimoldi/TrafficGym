from abc import ABC, abstractmethod
from trafficgym.engine.ports.simulation import SimulationPort


class Experiment(ABC):
    """Base class for all TrafficGym experiments.

    Subclasses must implement :meth:`run`, which drives the simulation by calling
    adapter step methods (e.g. ``adapter.run_time``, ``adapter.run_steps``).
    Observations declared via :meth:`subscribe` are collected automatically on
    every tick; one-off values can be emitted with :meth:`record`.
    """

    def __init__(self) -> None:
        self._subscriptions: list[tuple[str, str, str, str | None]] = []
        self._manual: dict[str, str] = {}

    def subscribe(self, name: str, domain: str, getter: str, object_id: str | None) -> None:
        """Register a recurring observation to be collected on every simulation tick.

        Args:
            name: Subscription fingerprint / label used in analytics.
            domain: SUMO domain (e.g. ``"lane"``, ``"edge"``).
            getter: SUMO getter method name (e.g. ``"getLastStepOccupancy"``).
            object_id: SUMO object identifier, or ``None`` for network-wide getters.
        """
        self._subscriptions.append((name, domain, getter, object_id))

    def record(self, name: str, value: object) -> None:
        """Emit a one-shot observation that is included in the next :meth:`poll` result.

        Useful for recording computed values or controller state that are not
        directly queryable via SUMO. The value is cleared after each poll.

        Args:
            name: Subscription fingerprint / label used in analytics.
            value: Any value; converted to ``str`` before storage.
        """
        self._manual[name] = str(value)

    def poll(self, adapter: SimulationPort) -> dict[str, str]:
        """Collect all subscribed observations plus any pending :meth:`record` values.

        Called automatically by the platform on every tick — experiments do not
        need to call this directly.

        Returns:
            Mapping of subscription name → string value for the current tick.
        """
        result = {
            name: adapter.query(domain, getter, object_id, {})
            for name, domain, getter, object_id in self._subscriptions
        }
        result.update(self._manual)
        self._manual.clear()
        return result

    @abstractmethod
    def run(self, adapter: SimulationPort) -> None:
        """Drive the simulation to completion.

        Implement this method to define the experiment logic. Use
        ``adapter.run_time``, ``adapter.run_steps``, or ``adapter.run_until_empty``
        to advance the simulation. The method should return when the experiment
        is finished; the adapter is closed automatically by the platform.

        Args:
            adapter: The simulation adapter bound to this execution.
        """
        ...
