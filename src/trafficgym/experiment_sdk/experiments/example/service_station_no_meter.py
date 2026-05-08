from trafficgym.experiment_sdk.experiments.base import Experiment
from trafficgym.engine.ports.simulation import SimulationPort

class service_station_no_meter(Experiment):
    def run(self, adapter: SimulationPort) -> None:
        tls_id = "TL0"

        self.subscribe("time", "simulation", "getTime", None)
        self.subscribe("last occupancy interval", "inductionloop", "getLastIntervalOccupancy", "e1_1")

        adapter.apply("trafficlight", "setProgram", tls_id, { "programID": "off" })

        adapter.run_time(300)

        self.subscribe("last step occupancy", "inductionloop", "getLastStepOccupancy", "e1_1")

        adapter.run_time(2400)
