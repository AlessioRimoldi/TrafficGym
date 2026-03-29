from trafficgym.experiment_sdk.experiments.base import Experiment


class service_station_no_meter(Experiment):
    async def run_experiment(self) -> None:
        tls_id = "TL0"

        await self.run.subscribe("simulation", "getTime", None)
        await self.run.subscribe(
            "inductionloop", "getLastIntervalOccupancy", "e1_1"
        )

        await self.run.tls.set_program(tls_id, "off")

        await self.run.run_time(300)

        # controller = await self.run.tls.meter_controller(tls_id, "e1_1")
        await self.run.subscribe(
            "inductionloop", "getLastStepOccupancy", "e1_1"
        )

        await self.run.run_time(2400)

        # controller.cancel()
