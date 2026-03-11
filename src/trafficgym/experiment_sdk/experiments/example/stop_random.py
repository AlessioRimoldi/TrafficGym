from trafficgym.experiment_sdk.experiments.base import Experiment
from trafficgym.experiment_sdk import sdk

import ast
import random
import logging

class stop_random(Experiment):
    async def run_experiment(self) -> None:
        tls_id = "TL0"

        await self.run.subscribe(
            "trafficlight", "getRedYellowGreenState", tls_id
        )
        eastbound_veh_ids = await self.run.subscribe("edge", "getLastStepVehicleIDs", "W2J")
        await self.run.subscribe("trafficlight", "getSpentDuration", tls_id)

        await self.run.run_time(20)

        for _ in range(1):
            await self.run.tls.set_signal(tls_id, "rGrG")
            await self.run.run_time(30)
            await self.run.tls.set_signal(tls_id, "GrGr")
            await self.run.run_time(30)

        await self.run.tls.set_signal(tls_id, "rGrG")


        # Get list of vehicles heading east on the western edge.
        eastbound_veh_ids_fetch = await self.run.fetch_subscription(eastbound_veh_ids.fingerprint, immediate_collect=False)
        if eastbound_veh_ids_fetch.data is None:  # checks for None and empty queue
            raise RuntimeError("Expected data!")

        try:
            veh_ids = ast.literal_eval(str(eastbound_veh_ids_fetch.data))
        except Exception as e:
            raise ValueError("Expected convertible to tuple") from e

        chosen_veh_id = str(random.choice(veh_ids))

        logging.info(chosen_veh_id)

        await self.run.apply_actions(sdk.ActionBundle([
            sdk.Action("vehicle", "setSpeed", chosen_veh_id, [("speed", sdk.Value(0.0))]),
            sdk.Action("vehicle", "setSignals", chosen_veh_id, [("signals", sdk.Value((1 << 2) + (1 << 10)))])
        ]))

        logging.info(f"Selected and stopped {chosen_veh_id}.")

        await self.run.run_time(30)

        await self.run.apply_actions(sdk.ActionBundle([
            sdk.Action("vehicle", "setSpeed", chosen_veh_id, [("speed", sdk.Value(-1.0))]),
            sdk.Action("vehicle", "setSignals", chosen_veh_id, [("signals", sdk.Value(-1))])
        ]))

        for _ in range(5):
            await self.run.tls.set_signal(tls_id, "rGrG")
            await self.run.run_time(50)
            await self.run.tls.set_signal(tls_id, "GrGr")
            await self.run.run_time(30)

        # await self.run.close_run()
