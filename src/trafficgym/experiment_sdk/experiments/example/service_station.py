from trafficgym.experiment_sdk.experiments.base import Experiment
from trafficgym.engine.ports.simulation import SimulationPort
from trafficgym.engine.control.controllers import RampMeterController

class service_station(Experiment):
    def run(self, adapter: SimulationPort) -> None:
        tls_id = "TL0"

        self.subscribe("time", "simulation", "getTime", None)
        self.subscribe("last occupancy interval", "inductionloop", "getLastIntervalOccupancy", "e1_1")

        adapter.apply("trafficlight", "setProgram", tls_id, { "programID": "off" })

        adapter.run_time(300)

        ramp_meter_controller = RampMeterController(tls_id, "e1_1") 

        adapter.register(ramp_meter_controller)

        self.subscribe("last step occupancy", "inductionloop", "getLastStepOccupancy", "e1_1")

        adapter.run_time(2400)

        adapter.deregister(ramp_meter_controller)

