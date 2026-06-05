from typing import Any

from trafficgym.engine.ports.simulation import SimulationPort, Observation, Params


class CountingAdapter(SimulationPort):
    """Dry-run adapter that counts steps without simulating."""

    def __init__(self) -> None:
        super().__init__(1000)
        self.open_ended = False

    def start(self) -> None: pass
    def close(self) -> None: pass

    def tick(self) -> tuple[int, float, Observation]:
        self.step += 1
        return self.step, float(self.step), {}

    def query(self, domain: str, getter_name: str, object_id: str | None, args: Params) -> str:
        return "0"

    def apply(self, domain: str, setter_name: str, object_id: str | None, args: Params) -> None:
        pass

    def run_until_empty(self) -> None:
        self.open_ended = True


def count_experiment_steps(artefact: Any) -> int:
    """Execute an experiment artefact against a CountingAdapter and return the step count.
    Returns -1 if any phase is open-ended (run_until_empty) or the file cannot be loaded."""
    from trafficgym.engine.experiment import Experiment as BaseExperiment
    try:
        artefact.file.seek(0)
        source = artefact.file.read()
        if isinstance(source, bytes):
            source = source.decode()
        ns: dict[str, Any] = {}
        exec(compile(source, "<experiment>", "exec"), ns)
        cls = next(
            (v for v in ns.values()
             if isinstance(v, type) and issubclass(v, BaseExperiment) and v is not BaseExperiment),
            None,
        )
        if cls is None:
            return -1
        adapter = CountingAdapter()
        cls().run(adapter)
        return -1 if adapter.open_ended else adapter.step
    except Exception:
        return -1
