from __future__ import annotations
import asyncio
import grpc
import logging

from collections import deque, defaultdict
import random

from ..api import engine_pb2, engine_pb2_grpc
from google.protobuf.struct_pb2 import Value

from dataclasses import dataclass

from typing import AsyncIterable, Any

import sys

errors: list[str] = []
warnings: list[str] = []


@dataclass
class StoreEntry:
    step: int
    value: str


async def handle_teardown(
    task: asyncio.Task[object], streaming: asyncio.Future[Any]
) -> None:
    exception = task.exception() if not task.cancelled() else None

    if isinstance(exception, grpc.RpcError):
        if exception.code() == grpc.StatusCode.UNKNOWN:
            logging.critical(
                "An unknown gRPC error occurred, check server console."
            )
            # sys.exit(-1)
            # raise exception
        elif exception.code() == grpc.StatusCode.UNAVAILABLE:
            logging.critical("Lost connection to the server.")
            sys.exit()
        else:
            raise exception

    if not streaming.done():
        streaming.cancel()

    try:
        await streaming
    except (asyncio.CancelledError, Exception):
        pass


async def set_signal(
    stub: engine_pb2_grpc.EngineServiceStub,
    run_id: str,
    signal_id: str,
    state: str,
) -> engine_pb2.ApplyActionsResponse:
    return stub.ApplyActions(
        engine_pb2.ActionBundle(
            run_id=run_id,
            step=0,
            actions=[
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


async def main() -> None:
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
                # sumo_binary="sumo",
                sumo_binary="sumo-gui",
                step_length_ms=10,
            )
        )
        run_id = cr.run_id

        logging.debug("Run created")

        subscriptions_store: dict[str, deque[StoreEntry]] = defaultdict(
            lambda: deque(maxlen=10)
        )

        async def handle_stream(
            stream: AsyncIterable[engine_pb2.TelemetryFrame],
            prefix: str = "",
            store: dict[str, deque[StoreEntry]] | None = None,
            print_filter: list[str] | None = None,
        ) -> None:
            # debug = logging.getLogger().getEffectiveLevel() == logging.DEBUG
            async for frame in stream:
                kv = {}
                for m in frame.metrics:
                    # value = m.value.SerializeToString()
                    field = m.value.WhichOneof("kind")
                    if field == "string_value":
                        value = m.value.string_value
                    elif field == "number_value":
                        value = str(m.value.number_value)
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
                        store.setdefault(m.key, deque()).append(
                            StoreEntry(frame.step, value)
                        )

                    if print_filter is None or m.key in print_filter:
                        # if debug or print_filter is None or m.key in print_filter:
                        kv[m.key] = value

                if kv:
                    print(prefix, frame.step, f"{frame.sim_time_s}s", kv)

        telemetry_stream: AsyncIterable[engine_pb2.TelemetryFrame] = (
            stub.StreamTelemetry(engine_pb2.StreamRequest(run_id=run_id))
        )

        subscription_stream: AsyncIterable[engine_pb2.TelemetryFrame] = (
            stub.StreamSubscriptions(engine_pb2.StreamRequest(run_id=run_id))
        )

        streaming = asyncio.gather(
            handle_stream(
                stream=telemetry_stream,
                prefix="T: ",
                store=None,
                print_filter=["Info", "Error", "Warning"],
            ),
            handle_stream(
                stream=subscription_stream,
                prefix="S: ",
                store=subscriptions_store,
                # print_filter=None,
                # print_filter=[],
                print_filter=[
                    "trafficlight.getRedYellowGreenState_TL0",
                    "trafficlight.getSpentDuration_TL0",
                    "simulation.getTime_",
                ],
            ),
        )

        async def wait_for_step(
            sub_store: deque[StoreEntry], step: int
        ) -> None:
            while sub_store[-1].step < step:
                await asyncio.sleep(0)

        async def run_tls_program(
            run_id: str,
            tls_id: str,
            phases: list[str],
            durations: list[int],
            inital_step: int,
        ) -> asyncio.Task[None]:
            """Run through the phases provided as strings.
            Once the corresponding duration has elapsed since
            the signal changed, switch to the next phase."""
            assert len(phases) == len(durations)

            get_time_subscription: engine_pb2.SubscribeResponse = (
                await stub.Subscribe(
                    engine_pb2.SubscribeRequest(
                        run_id=run_id,
                        domain="simulation",
                        getter_name="getTime",
                    )
                )
            )

            await set_signal(stub, run_id, tls_id, phases[0])

            time_subscription_name = (
                get_time_subscription.subscription_name_or_fingerprint
            )

            # for i in range(len(phases)):
            interrupt_event_stream: AsyncIterable[
                engine_pb2.InterruptEvent | None
            ] = stub.RegisterInterrupt(
                engine_pb2.RegisterInterruptRequest(
                    run_id=run_id,
                    trigger_metric=engine_pb2.MetricNameAndValue(
                        name=time_subscription_name,
                        value=Value(
                            number_value=(durations[0] + inital_step)
                        ),
                    ),
                )
            )
            logging.debug("TLS Program interrupt registration sent")

            async def tls_program_async() -> None:
                i = 0
                async for interrupt_event in interrupt_event_stream:
                    if not interrupt_event:
                        logging.debug("TLS Program interrupt cancelled")
                        return
                    logging.debug(
                        f"TLS Program interrupt triggered. Event: {interrupt_event.event_id}"
                    )
                    i += 1 # start at 1 because we have already initialised state 0

                    try:
                        observed_value = int(float(interrupt_event.observed_value.string_value))
                    except:
                        logging.warning(f"Failed to read interrupt observed value")
                        observed_value = 0

                    logging.debug("ACKING THE INTERRUPT")
                    await stub.AcknowledgeInterrupt(
                        engine_pb2.AcknowledgeInterruptRequest(
                            run_id=run_id,
                            interrupt_id=interrupt_event.interrupt_id,
                            event_id=interrupt_event.event_id,
                            actions=engine_pb2.ActionBundle(
                                run_id=run_id,
                                actions=[
                                    engine_pb2.Action(
                                        setter=engine_pb2.GenericSetter(
                                            domain="trafficlight",
                                            setter_name="setRedYellowGreenState",
                                            object_id=tls_id,
                                            value=phases[
                                                i % len(phases)
                                            ],
                                        )
                                    )
                                ],
                            ),
                            new_interrupt_conditions=engine_pb2.MetricNameAndValue(
                                name=time_subscription_name,
                                value=Value(
                                    string_value=str(float(
                                        durations[i % len(phases)] + observed_value 
                                    ))
                                ),
                            ),
                        )
                    )
                    logging.debug("Interrput Acked")

            return asyncio.create_task(tls_program_async(), name="tls_program")

        async def scenario_stop_random() -> None:
            eastbound_veh_ids = "eastbound_veh_ids"

            await stub.Subscribe(
                engine_pb2.SubscribeRequest(
                    # name="My Favorite Traffic Signal",
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
            await stub.Subscribe(
                engine_pb2.SubscribeRequest(
                    run_id=run_id,
                    domain="trafficlight",
                    getter_name="getSpentDuration",
                    object_id=tls_id,
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

            tup = eval(deq[-1].value)

            chosen_veh_id = str(random.choice(tup))

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

        async def scenario_stop_random_async_tls() -> None:
            eastbound_veh_ids = "eastbound_veh_ids"

            await stub.Subscribe(
                engine_pb2.SubscribeRequest(
                    name=eastbound_veh_ids,
                    run_id=run_id,
                    domain="edge",
                    getter_name="getLastStepVehicleIDs",
                    object_id="W2J",
                )
            )
            await stub.Subscribe(
                engine_pb2.SubscribeRequest(
                    run_id=run_id,
                    domain="trafficlight",
                    getter_name="getRedYellowGreenState",
                    object_id=tls_id,
                )
            )

            # tls_program = asyncio.create_task(
            tls_program = await run_tls_program(
                run_id,
                tls_id,
                ["rGrG", "ryry", "GrGr", "yryr"],
                [30, 3, 30, 3],
                0
            )
            # )

            response: engine_pb2.RunResponse = await stub.Run(
                engine_pb2.RunRequest(run_id=run_id, max_time=100)
            )
            new_step = response.new_step

            # Get list of vehicles heading east on the western edge.
            deq = subscriptions_store.get(eastbound_veh_ids)
            if not deq:  # checks for None and empty queue
                raise RuntimeError("Expected data!")

            logging.debug(
                f"Will wait until telemetry reception for step {new_step}"
            )
            await asyncio.wait_for(wait_for_step(deq, new_step), 10)

            tup = eval(deq[-1].value)

            chosen_veh_id = str(random.choice(tup))

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

            await stub.Run(engine_pb2.RunRequest(run_id=run_id, max_time=100))

            tls_program.cancel()

            await stub.CloseRun(engine_pb2.CloseRunRequest(run_id=run_id))

        scenario = asyncio.create_task(scenario_stop_random_async_tls())
        # scenario = asyncio.create_task(scenario_stop_random())

        try:
            await scenario
        except Exception as e:
            # Exceptions handled in teardown
            errors.append(str(e))
            pass

        await handle_teardown(scenario, streaming)

        for err in errors:
            logging.info(f"Error: {err}")

        for warn in warnings:
            logging.info(f"Warning: {warn}")

        logging.info(
            f"Execution terminated with {len(errors)} error(s) and {len(warnings)} warning(s)."
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    asyncio.run(main())

    sys.exit(0)
