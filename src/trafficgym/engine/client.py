from __future__ import annotations
import asyncio
import grpc
import logging

from collections import deque, defaultdict
import random

from ..api import engine_pb2, engine_pb2_grpc # type: ignore[import]

from typing import AsyncIterator

import sys

errors: list[str] = []
warnings: list[str] = []


def raise_async_except(task):
    if task.cancelled():
        logging.error(f"{task.get_name()} was cancelled")
    elif task.exception() is not None:
        exception = task.exception()
        if isinstance(exception, grpc.RpcError):
            if exception.code() == grpc.StatusCode.UNKNOWN:
                logging.critical(
                    "An unknown gRPC error occurred, check server console."
                )
                sys.exit(-1)
                # raise exception
            elif exception.code() == grpc.StatusCode.UNAVAILABLE:
                logging.critical("Lost connection to the server.")
                sys.exit()
            else:
                raise exception
        else:
            logging.error(f"An exception occurred: {exception}")
            raise exception


async def set_signal(stub, run_id, signal_id, state):
    return stub.ApplyActions(
        engine_pb2.ActionBundle(
            run_id=run_id,
            step=0,
            actions=[
                # engine_pb2.Action(
                #     setter=engine_pb2.GenericSetter(
                #         domain="trafficlight",
                #         setter_name="setPhase",
                #         object_id=tls_id,
                #         value="1",
                #     )
                # ),
                engine_pb2.Action(
                    setter=engine_pb2.GenericSetter(
                        domain="trafficlight",
                        setter_name="setRedYellowGreenState",
                        object_id=signal_id,
                        value=state,
                    )
                )
            ],
        )
    )


async def main():
    sumocfg_path = (
        "/home/diego/documents/"
        # "/home/r/Code"
        "TrafficGym/sumo_files/single_intersection/sim.sumocfg"
    )

    tls_id = "TL0"

    async with grpc.aio.insecure_channel("127.0.0.1:50051") as channel:
        stub = engine_pb2_grpc.EngineServiceStub(channel)

        cr = await stub.CreateRun(
            engine_pb2.CreateRunRequest(
                sumocfg_path=sumocfg_path,
                sumo_binary="sumo",
                step_length_ms=100,
            )
        )
        run_id = cr.run_id

        subscriptions_store: dict[str, deque[str]] = defaultdict(lambda: deque(maxlen=10))

        async def scenario():
            eastbound_veh_ids = "eastbound_veh_ids"

            # await stub.Subscribe(
            #     engine_pb2.SubscribeRequest(
            #         run_id=run_id,
            #         domain="trafficlight",
            #         getter_name="getPhase",
            #         object_id=tls_id,
            #     )
            # )
            # await stub.Subscribe(
            #     engine_pb2.SubscribeRequest(
            #         run_id=run_id,
            #         domain="lane",
            #         getter_name="getLastStepVehicleNumber",
            #         object_id=":J0_0_0",
            #     )
            # )
            # await stub.Subscribe(
            #     engine_pb2.SubscribeRequest(
            #         run_id=run_id, domain="vehicle", getter_name="getIDList"
            #     )
            # )
            await stub.Subscribe(
                engine_pb2.SubscribeRequest(
                    name="My Favorite Traffic Signal",
                    run_id=run_id,
                    domain="trafficlight",
                    getter_name="getRedYellowGreenState",
                    object_id=tls_id,
                )
            )
            await stub.Subscribe(
                engine_pb2.SubscribeRequest(
                    name=eastbound_veh_ids,
                    run_id=run_id,
                    domain="edge",
                    getter_name="getLastStepVehicleIDs",
                    object_id="W2J",
                )
            )
            await stub.Run(engine_pb2.RunRequest(run_id=run_id, max_time=20))
            for _ in range(1):
                await set_signal(stub, run_id, tls_id, "rGrG")
                await stub.Run(
                    engine_pb2.RunRequest(run_id=run_id, max_time=30)
                )
                await set_signal(stub, run_id, tls_id, "GrGr")
                await stub.Run(
                    engine_pb2.RunRequest(run_id=run_id, max_time=30)
                )

            await set_signal(stub, run_id, tls_id, "rGrG")

            # Get list of vehicles heading east on the western edge.
            deq = subscriptions_store.get(eastbound_veh_ids)
            if not deq:  # checks for None and empty queue
                raise RuntimeError("Expected data!")

            tup = eval(deq[-1])

            chosen_veh_id = random.choice(tup)

            logging.info(chosen_veh_id)

            await stub.ApplyActions(
                engine_pb2.ActionBundle(
                    run_id=run_id,
                    step=0,
                    actions=[
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="vehicle",
                                setter_name="setSpeed",
                                object_id=chosen_veh_id,
                                value="0",
                            )
                        ),
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="vehicle",
                                setter_name="setSignals",
                                object_id=chosen_veh_id,
                                value=str((1 << 2) + (1 << 10)),
                                # emergency signal and door right open
                            )
                        ),
                    ],
                )
            )

            logging.info(f"Selected and stopped {chosen_veh_id}.")

            await stub.Run(engine_pb2.RunRequest(run_id=run_id, max_time=30))

            await stub.ApplyActions(
                engine_pb2.ActionBundle(
                    run_id=run_id,
                    step=0,
                    actions=[
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="vehicle",
                                setter_name="setSpeed",
                                object_id=chosen_veh_id,
                                value="-1",
                            )
                        ),
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="vehicle",
                                setter_name="setSignals",
                                object_id=chosen_veh_id,
                                value="-1",
                            )
                        ),
                    ],
                )
            )

            for _ in range(5):
                await set_signal(stub, run_id, tls_id, "rGrG")
                await stub.Run(
                    engine_pb2.RunRequest(run_id=run_id, max_time=50)
                )
                await set_signal(stub, run_id, tls_id, "GrGr")
                await stub.Run(
                    engine_pb2.RunRequest(run_id=run_id, max_time=30)
                )

            await stub.CloseRun(engine_pb2.CloseRunRequest(run_id=run_id))

        asyncio.create_task(scenario()).add_done_callback(
            raise_async_except
        )  # crash hard for now

        telemetry_stream: AsyncIterator[engine_pb2.TelemetryFrame] = (
            stub.StreamTelemetry(engine_pb2.StreamRequest(run_id=run_id))
        )

        subscription_stream: AsyncIterator[engine_pb2.TelemetryFrame] = (
            stub.StreamSubscriptions(engine_pb2.StreamRequest(run_id=run_id))
        )

        async def handle_stream(
            stream: AsyncIterator[engine_pb2.TelemetryFrame],
            prefix="",
            store: dict[str, deque[str]] | None = None,
            print_filter: list[str] | None = None,
        ):
            async for frame in stream:
                kv = {}
                for m in frame.metrics:
                    field = m.WhichOneof("value")
                    if field == "string_value":
                        value = m.string_value
                    elif field == "double_value":
                        value = str(m.double_value)
                    else:
                        logging.warning(
                            f"Stream frame value is not a string or a float!"
                        )
                        continue

                    if m.key == "Error":
                        errors.append(value)
                        continue
                    elif m.key == "Warning":
                        warnings.append(value)
                        continue

                    if store is not None:
                        store.setdefault(m.key, deque()).append(value)

                    if print_filter is None or m.key in print_filter:
                        kv[m.key] = value

                if kv:
                    print(prefix, frame.step, f"{frame.sim_time_s}s", kv)

        try:
            await asyncio.gather(
                handle_stream(
                    stream=telemetry_stream,
                    prefix="T: ",
                    store=None,
                    print_filter=["Info", "Error", "Warning"]
                ),
                handle_stream(
                    stream=subscription_stream,
                    prefix="S: ",
                    store=subscriptions_store,
                    print_filter=[]
                ),
            )
        except Exception as e:
            logging.error(str(e))
            errors.append(str(e))


        logging.info(
            f"Execution terminated with {len(errors)} error(s) and {len(warnings)} warning(s)."
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    asyncio.run(main())

    sys.exit(0)
