from trafficgym.experiment_sdk.experiments.base import Experiment
from trafficgym.engine.ports.simulation import SimulationPort

import ast
import random
import logging


class stop_random(Experiment):
    def run(self, adapter: SimulationPort) -> None:
        tls_id = "TL0"

        self.subscribe("TL0 State", "trafficlight", "getRedYellowGreenState", tls_id)
        self.subscribe("eastbound_veh_ids", "edge", "getLastStepVehicleIDs", "W2J")
        self.subscribe("TL0 Spent", "trafficlight", "getSpentDuration", tls_id)

        adapter.run_time(20)

        for _ in range(1):
            adapter.apply("trafficlight", "setRedYellowGreenState", tls_id, {"state": "rGrG"})
            adapter.run_time(30)
            adapter.apply("trafficlight", "setRedYellowGreenState", tls_id, {"state": "GrGr"})
            adapter.run_time(30)

        adapter.apply("trafficlight", "setRedYellowGreenState", tls_id, {"state": "rGrG"})

        # Get list of vehicles heading east on the western edge.
        eastbound_veh_ids_fetch = adapter.query("edge", "getLastStepVehicleIDs", "W2J", {})

        if (
            eastbound_veh_ids_fetch is None
        ):  # checks for None and empty queue
            raise RuntimeError("Expected data!")

        try:
            veh_ids = ast.literal_eval(str(eastbound_veh_ids_fetch))
        except Exception as e:
            raise ValueError("Expected convertible to tuple") from e

        chosen_veh_id = str(random.choice(veh_ids))

        logging.info(chosen_veh_id)

        adapter.apply("vehicle", "setSpeed", chosen_veh_id, {"speed": 0})
        adapter.apply("vehicle", "setSignals", chosen_veh_id, {"signals": (1 << 2) + (1 << 10)})

        logging.info(f"Selected and stopped {chosen_veh_id}.")

        adapter.run_time(30)

        adapter.apply("vehicle", "setSpeed", chosen_veh_id, {"speed": -1})
        adapter.apply("vehicle", "setSignals", chosen_veh_id, {"signals": -1})

        for _ in range(5):
            adapter.apply("trafficlight", "setRedYellowGreenState", tls_id, {"state": "rGrG"})
            adapter.run_time(50)
            adapter.apply("trafficlight", "setRedYellowGreenState", tls_id, {"state": "GrGr"})
            adapter.run_time(30)
