from trafficgym.experiment_sdk.experiments.base import Experiment
from trafficgym.engine.ports.simulation import SimulationPort
from trafficgym.engine.control.controllers import RampMeterController


class service_station(Experiment):
    def run(self, adapter: SimulationPort) -> None:
        tls_id = "TL0"
        det_id = "e1_1"

        self.subscribe("time", "simulation", "getTime", None)
        self.subscribe("last occupancy interval", "inductionloop", "getLastIntervalOccupancy", det_id)

        adapter.apply("trafficlight", "setProgram", tls_id, {"programID": "off"})
        adapter.run_time(300)

        self.subscribe("last step occupancy", "inductionloop", "getLastStepOccupancy", det_id)

        with adapter.controlled(
            RampMeterController(),
            observe=lambda a, _: {"occupancy": float(a.query("inductionloop", "getLastIntervalOccupancy", det_id, {}))},
            actuate=lambda a, r: a.apply("trafficlight", "setProgram", tls_id, {"programID": r["program_id"]}) if "program_id" in r else None,
        ):
            adapter.run_time(2400)

        # Controller deregisters on context manager exit

        adapter.run_time(200)