from __future__ import annotations
from grpc.aio import insecure_channel
from trafficgym.api import engine_pb2_grpc
from trafficgym.engine.client.driver import EngineDriver
from trafficgym.experiment_sdk.experiments.example.stop_random import (
    stop_random,
)
import sys
import asyncio
import logging

sumocfg_path = (
    "/home/diego/documents/"
    # "/home/r/Code"
    "TrafficGym/sumo_files/single_intersection/sim.sumocfg"
    # "TrafficGym/sumo_files/service_station/service_station.sumocfg"
)


async def main() -> None:
    ExperimentClass = stop_random

    async with insecure_channel("127.0.0.1:50051") as channel:
        stub = engine_pb2_grpc.EngineServiceStub(channel)

        engine_driver = EngineDriver(stub)

        async with engine_driver.create_run(
            str(sumocfg_path), "sumo", 1000
        ) as run:
            await ExperimentClass(run).run_experiment()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    asyncio.run(main())

    sys.exit(0)
