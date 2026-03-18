from trafficgym.experiment_sdk.experiments.base import Experiment
from trafficgym.experiment_sdk import sdk

import ast
import random
import logging


class stop_random_async_tls(Experiment):
    async def run_experiment(self) -> None:
        tls_id = "TL0"

        await self.run.subscribe(
            "trafficlight", "getRedYellowGreenState", tls_id
        )
        eastbound_veh_ids = await self.run.subscribe(
            "edge", "getLastStepVehicleIDs", "W2J"
        )
        await self.run.subscribe("trafficlight", "getSpentDuration", tls_id)

        tls_program = await self.run.tls.run_tls_program(
            tls_id,
            ["rGrG", "ryry", "GrGr", "yryr"],
            [30, 3, 30, 3],
            0,
        )

        await self.run.run_time(100)

        # Get list of vehicles heading east on the western edge.
        eastbound_veh_ids_fetch = await self.run.fetch_subscription(
            eastbound_veh_ids.fingerprint, immediate_collect=False
        )
        if (
            eastbound_veh_ids_fetch.data is None
        ):  # checks for None and empty queue
            raise RuntimeError("Expected data!")

        try:
            veh_ids = ast.literal_eval(str(eastbound_veh_ids_fetch.data))
        except Exception as e:
            raise ValueError("Expected convertible to tuple") from e

        chosen_veh_id = str(random.choice(veh_ids))

        logging.info(chosen_veh_id)

        await self.run.apply_actions(
            sdk.ActionBundle(
                [
                    sdk.Action(
                        "vehicle",
                        "setSpeed",
                        chosen_veh_id,
                        [sdk.Parameter("speed", sdk.Value(0.0))],
                    ),
                    sdk.Action(
                        "vehicle",
                        "setSignals",
                        chosen_veh_id,
                        [
                            sdk.Parameter(
                                "signals", sdk.Value((1 << 2) + (1 << 10))
                            )
                        ],
                    ),
                ]
            )
        )

        logging.info(f"Selected and stopped {chosen_veh_id}.")

        await self.run.run_time(30)

        await self.run.apply_actions(
            sdk.ActionBundle(
                [
                    sdk.Action(
                        "vehicle",
                        "setSpeed",
                        chosen_veh_id,
                        [sdk.Parameter("speed", sdk.Value(-1.0))],
                    ),
                    sdk.Action(
                        "vehicle",
                        "setSignals",
                        chosen_veh_id,
                        [sdk.Parameter("signals", sdk.Value(-1))],
                    ),
                ]
            )
        )

        await self.run.run_time(100)

        tls_program.cancel()
