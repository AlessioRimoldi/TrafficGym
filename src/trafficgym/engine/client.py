from __future__ import annotations
import asyncio
import logging

from collections import deque, defaultdict
import random

import grpc
from grpc.aio import (
    UnaryUnaryClientInterceptor,
    UnaryStreamClientInterceptor,
    UnaryUnaryCall,
    UnaryStreamCall,
    ClientCallDetails,
    insecure_channel,
    ClientInterceptor,
)
from google.protobuf.message import Message

from ..api import engine_pb2, engine_pb2_grpc
from google.protobuf.struct_pb2 import Value

from dataclasses import dataclass
from enum import Enum, auto

from typing import AsyncIterable, Any, Callable, cast

import sys

errors: list[str] = []
warnings: list[str] = []

InterruptStream = AsyncIterable[engine_pb2.InterruptEvent | None]


class UnaryUnaryInterceptor(UnaryUnaryClientInterceptor):  # type: ignore[type-arg]
    async def intercept_unary_unary(
        self,
        continuation: Callable[
            [ClientCallDetails, Message], UnaryUnaryCall[Any, Any]
        ],
        client_call_details: ClientCallDetails,
        request: Message,
    ) -> UnaryUnaryCall[Any, Any]:
        logging.debug(f"Sending UU Request: {type(request).__name__}")

        return await continuation(client_call_details, request)  # type: ignore[no-any-return]


class UnaryStreamInterceptor(UnaryStreamClientInterceptor):  # type: ignore[type-arg]
    async def intercept_unary_stream(
        self,
        continuation: Callable[
            [ClientCallDetails, Message], UnaryStreamCall[Any, Any]
        ],
        client_call_details: ClientCallDetails,
        request: Message,
    ) -> UnaryStreamCall[Any, Any]:
        logging.debug(f"Sending US Request: {type(request).__name__}")

        return await continuation(client_call_details, request)  # type: ignore[no-any-return, misc]


@dataclass
class StoreEntry:
    step: int
    value: Value


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
                        value=Value(string_value=state),
                    )
                )
            ],
        )
    )


async def main() -> None:
    sumocfg_path = (
        "/home/diego/documents/"
        # "/home/r/Code"
        # "TrafficGym/sumo_files/single_intersection/sim.sumocfg"
        "TrafficGym/sumo_files/service_station/service_station.sumocfg"
    )

    tls_id = "TL0"

    async with insecure_channel(
        "127.0.0.1:50051",
        interceptors=[
            cast(ClientInterceptor, UnaryUnaryInterceptor()),
            cast(ClientInterceptor, UnaryStreamInterceptor()),
        ],
    ) as channel:
        stub = engine_pb2_grpc.EngineServiceStub(channel)

        cr = await stub.CreateRun(
            engine_pb2.CreateRunRequest(
                sumocfg_path=sumocfg_path,
                # sumo_binary="sumo",
                sumo_binary="sumo-gui",
                step_length_ms=100,
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
                    value = m.value

                    if m.key == "Error":
                        errors.append(value.string_value)
                        continue
                    elif m.key == "Warning":
                        warnings.append(value.string_value)
                        continue

                    if store is not None:
                        store.setdefault(m.key, deque()).append(
                            StoreEntry(frame.step, value)
                        )

                    if print_filter is None or m.key in print_filter:
                        # if debug or print_filter is None or m.key in print_filter:
                        kv[m.key] = (
                            value.string_value
                            if value.string_value != ""
                            else str(value.number_value)
                        )

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
                print_filter=[
                    "CAPass",
                    "CAFlow",
                ],
                # print_filter=[
                #     "trafficlight.getRedYellowGreenState_TL0",
                #     "trafficlight.getSpentDuration_TL0",
                #     "simulation.getTime_",
                # ],
            ),
        )

        async def wait_for_step(
            sub_store: deque[StoreEntry], step: int
        ) -> None:
            while sub_store[-1].step < step:
                await asyncio.sleep(0)

        async def wait_for_interrupt(
            stream: InterruptStream,
        ) -> engine_pb2.InterruptEvent | None:
            interrupt_id: str | None = None

            try:
                async for event in stream:
                    if event is None:
                        return None

                    interrupt_id = event.interrupt_id
                    return event
            except asyncio.CancelledError:
                if interrupt_id is not None:
                    logging.debug(
                        f"Cancelling from wait_for_interrupt() {interrupt_id}"
                    )
                    await stub.CancelInterrupt(
                        engine_pb2.CancelInterruptRequest(
                            run_id=run_id,
                            interrupt_id=interrupt_id,
                        )
                    )

            return None

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

            time_subscription_name = get_time_subscription.fingerprint

            # for i in range(len(phases)):
            interrupt_event_stream: AsyncIterable[
                engine_pb2.InterruptEvent | None
            ] = stub.RegisterInterrupt(
                engine_pb2.RegisterInterruptRequest(
                    run_id=run_id,
                    trigger_metric=engine_pb2.MetricNameAndValue(
                        name=time_subscription_name,
                        value=Value(number_value=(durations[0] + inital_step)),
                        op=engine_pb2.Operation.GEQ
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
                    i += 1  # start at 1 because we have already initialised state 0

                    try:
                        observed_value = (
                            interrupt_event.observed_value.number_value
                        )
                    except:
                        message = "Failed to read interrupt observed value"
                        logging.warning(message)
                        warnings.append(message)
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
                                            value=Value(
                                                string_value=phases[
                                                    i % len(phases)
                                                ]
                                            ),
                                        )
                                    )
                                ],
                            ),
                            new_interrupt_conditions=engine_pb2.MetricNameAndValue(
                                name=time_subscription_name,
                                value=Value(
                                    number_value=float(
                                        durations[i % len(phases)]
                                        + observed_value
                                    )
                                ),
                                op=engine_pb2.Operation.GEQ,
                            ),
                        )
                    )
                    logging.debug("Interrput Acked")

            return asyncio.create_task(tls_program_async(), name="tls_program")

        async def meter_controller(tls_id: str, det_id: str) -> None:
            high_traffic_interrupts: InterruptStream
            low_traffic_interrupts: InterruptStream

            class MeterState(Enum):
                OFF = auto()
                QUARTER = auto()
                TENTH = auto()
                CHOKE = auto()

            meter_state: MeterState = MeterState.OFF

            detector_nearside: engine_pb2.SubscribeResponse = (
                await stub.Subscribe(
                    engine_pb2.SubscribeRequest(
                        name="occup_main_detector_nearside",
                        run_id=run_id,
                        domain="inductionloop",
                        getter_name="getLastIntervalOccupancy",
                        object_id=det_id,
                    )
                )
            )

            await stub.ApplyActions(
                engine_pb2.ActionBundle(
                    run_id=run_id,
                    actions=[
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="trafficlight",
                                setter_name="setProgram",
                                object_id=tls_id,
                                value=Value(string_value="0"),
                            )
                        )
                    ],
                )
            )

            interrupt_event: engine_pb2.InterruptEvent | None = None

            while True:
                if meter_state is MeterState.OFF:
                    high_traffic_interrupts = stub.RegisterInterrupt(
                        engine_pb2.RegisterInterruptRequest(
                            run_id=run_id,
                            trigger_metric=engine_pb2.MetricNameAndValue(
                                name=detector_nearside.fingerprint,
                                value=Value(number_value=15.0),
                                op=engine_pb2.Operation.GEQ,
                            ),
                        )
                    )

                    interrupt_event = None

                    async for event in high_traffic_interrupts:
                        if not event:
                            return
                        interrupt_event = event
                        logging.info("Setting meter to 1/4 mode")
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
                                                setter_name="setProgram",
                                                object_id=tls_id,
                                                value=Value(
                                                    string_value="1"
                                                ),  # METER QUARTER
                                            )
                                        ),
                                    ],
                                ),
                            )
                        )
                        meter_state = MeterState.QUARTER
                        break

                    # if interrupt_event is not None:
                    #     # interrupt is already cancelled normally
                    #     await stub.CancelInterrupt(
                    #         request=engine_pb2.CancelInterruptRequest(
                    #             run_id=run_id,
                    #             interrupt_id=interrupt_event.interrupt_id,
                    #         )
                    #     )

                elif meter_state is MeterState.QUARTER:
                    high_traffic_interrupts = stub.RegisterInterrupt(
                        engine_pb2.RegisterInterruptRequest(
                            run_id=run_id,
                            trigger_metric=engine_pb2.MetricNameAndValue(
                                name=detector_nearside.fingerprint,
                                value=Value(number_value=20.0),
                                op=engine_pb2.Operation.GEQ,
                            ),
                        )
                    )

                    low_traffic_interrupts = stub.RegisterInterrupt(
                        engine_pb2.RegisterInterruptRequest(
                            run_id=run_id,
                            trigger_metric=engine_pb2.MetricNameAndValue(
                                name=detector_nearside.fingerprint,
                                value=Value(number_value=10.0),
                                op=engine_pb2.Operation.LEQ,
                            ),
                        )
                    )

                    high_traffic_task = asyncio.create_task(
                        wait_for_interrupt(high_traffic_interrupts)
                    )
                    low_traffic_task = asyncio.create_task(
                        wait_for_interrupt(low_traffic_interrupts)
                    )

                    done, pending = await asyncio.wait(
                        {high_traffic_task, low_traffic_task},
                        return_when="FIRST_COMPLETED",
                    )

                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            logging.debug(f"Cancelled {task}")

                    finished = done.pop()

                    interrupt_event = finished.result()
                    if interrupt_event is None:
                        return

                    if finished is high_traffic_task:
                        logging.info("Setting meter to 1/10 mode")
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
                                                setter_name="setProgram",
                                                object_id=tls_id,
                                                value=Value(
                                                    string_value="2"
                                                ),  # METER TENTHS
                                            )
                                        ),
                                    ],
                                ),
                            )
                        )
                        meter_state = MeterState.TENTH

                    else:
                        logging.info("Setting meter OFF")
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
                                                setter_name="setProgram",
                                                object_id=tls_id,
                                                value=Value(
                                                    string_value="off"
                                                ),  # METER OFF
                                            )
                                        ),
                                    ],
                                ),
                            )
                        )
                        meter_state = MeterState.OFF

                elif meter_state is MeterState.TENTH:
                    high_traffic_interrupts = stub.RegisterInterrupt(
                        engine_pb2.RegisterInterruptRequest(
                            run_id=run_id,
                            trigger_metric=engine_pb2.MetricNameAndValue(
                                name=detector_nearside.fingerprint,
                                value=Value(number_value=30.0),
                                op=engine_pb2.Operation.GEQ,
                            ),
                        )
                    )

                    low_traffic_interrupts = stub.RegisterInterrupt(
                        engine_pb2.RegisterInterruptRequest(
                            run_id=run_id,
                            trigger_metric=engine_pb2.MetricNameAndValue(
                                name=detector_nearside.fingerprint,
                                value=Value(number_value=15.0),
                                op=engine_pb2.Operation.LEQ,
                            ),
                        )
                    )

                    interrupt_event = None

                    high_traffic_task = asyncio.create_task(
                        wait_for_interrupt(high_traffic_interrupts)
                    )
                    low_traffic_task = asyncio.create_task(
                        wait_for_interrupt(low_traffic_interrupts)
                    )

                    done, pending = await asyncio.wait(
                        {high_traffic_task, low_traffic_task},
                        return_when="FIRST_COMPLETED",
                    )

                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            logging.debug(f"Cancelled {task}")

                    finished = done.pop()

                    interrupt_event = finished.result()
                    if interrupt_event is None:
                        return

                    if finished is high_traffic_task:
                        logging.info("Setting meter to CHOKE mode")
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
                                                setter_name="setProgram",
                                                object_id=tls_id,
                                                value=Value(
                                                    string_value="3"
                                                ),  # METER CHOKE
                                            )
                                        ),
                                    ],
                                ),
                            )
                        )
                        meter_state = MeterState.CHOKE

                    else:
                        logging.info("Setting meter to 1/4 mode")
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
                                                setter_name="setProgram",
                                                object_id=tls_id,
                                                value=Value(
                                                    string_value="1"
                                                ),  # METER QUARTER
                                            )
                                        ),
                                    ],
                                ),
                            )
                        )
                        meter_state = MeterState.QUARTER

                elif meter_state is MeterState.CHOKE:
                    low_traffic_interrupts = stub.RegisterInterrupt(
                        engine_pb2.RegisterInterruptRequest(
                            run_id=run_id,
                            trigger_metric=engine_pb2.MetricNameAndValue(
                                name=detector_nearside.fingerprint,
                                value=Value(number_value=25.0),
                                op=engine_pb2.Operation.LEQ,
                            ),
                        )
                    )

                    interrupt_event = None

                    async for event in low_traffic_interrupts:
                        if not event:
                            return
                        interrupt_event = event
                        logging.info("Setting meter to 1/10 mode")
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
                                                setter_name="setProgram",
                                                object_id="TL0",
                                                value=Value(
                                                    string_value="2"
                                                ),  # METER TENTHS
                                            )
                                        ),
                                    ],
                                ),
                            )
                        )
                        meter_state = MeterState.TENTH
                        break

                    # if interrupt_event is not None:
                    #     # interrupt is already cancelled normally
                    #     await stub.CancelInterrupt(
                    #         request=engine_pb2.CancelInterruptRequest(
                    #             run_id=run_id,
                    #             interrupt_id=interrupt_event.interrupt_id,
                    #         )
                    #     )

                else:
                    logging.warning("Unknown meter state, resetting to OFF")
                    meter_state = MeterState.OFF
                    await stub.ApplyActions(
                        engine_pb2.ActionBundle(
                            run_id=run_id,
                            actions=[
                                engine_pb2.Action(
                                    setter=engine_pb2.GenericSetter(
                                        domain="trafficlight",
                                        setter_name="setProgram",
                                        object_id="TL0",
                                        value=Value(string_value="0"),
                                    )
                                )
                            ],
                        )
                    )

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

            tup = eval(deq[-1].value.string_value)

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
                                value=Value(number_value=0),
                            )
                        ),
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="vehicle",
                                setter_name="setSignals",
                                object_id=chosen_veh_id,
                                value=Value(number_value=(1 << 2) + (1 << 10)),
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
                                value=Value(number_value=-1),
                            )
                        ),
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="vehicle",
                                setter_name="setSignals",
                                object_id=chosen_veh_id,
                                value=Value(number_value=-1),
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
                0,
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

            try:
                tup = eval(deq[-1].value.string_value)
            except Exception as e:
                logging.error(
                    f"Could no evaluate vehicle ID list into tuple. {e}"
                )

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
                                value=Value(number_value=0),
                            )
                        ),
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="vehicle",
                                setter_name="setSignals",
                                object_id=chosen_veh_id,
                                value=Value(number_value=(1 << 2) + (1 << 10)),
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
                                value=Value(number_value=-1),
                            )
                        ),
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="vehicle",
                                setter_name="setSignals",
                                object_id=chosen_veh_id,
                                value=Value(number_value=-1),
                            )
                        ),
                    ],
                )
            )

            run_response: engine_pb2.RunResponse = await stub.Run(
                engine_pb2.RunRequest(run_id=run_id, max_time=100)
            )
            new_step = run_response.new_step

            logging.debug(
                f"Will wait until telemetry reception for step {new_step}"
            )
            await asyncio.wait_for(wait_for_step(deq, new_step), 10)

            tls_program.cancel()

            await stub.CloseRun(engine_pb2.CloseRunRequest(run_id=run_id))

        async def scenario_service_station() -> None:

            # await stub.Subscribe(
            #     engine_pb2.SubscribeRequest(
            #         name="CAPass",
            #         run_id=run_id,
            #         domain="calibrator",
            #         getter_name="getPassed",
            #         object_id="ca_0",
            #     )
            # )
            # await stub.Subscribe(
            #     engine_pb2.SubscribeRequest(
            #         name="CAFlow",
            #         run_id=run_id,
            #         domain="calibrator",
            #         getter_name="getVehsPerHour",
            #         object_id="ca_0",
            #     )
            # )


            await stub.ApplyActions(
                engine_pb2.ActionBundle(
                    run_id=run_id,
                    actions=[
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="trafficlight",
                                setter_name="setProgram",
                                object_id=tls_id,
                                value=Value(string_value="off"),  # METER OFF
                            )
                        ),
                    ],
                )
            )

            await stub.Run(engine_pb2.RunRequest(run_id=run_id, max_time=300))

            controller = asyncio.create_task(meter_controller(tls_id, "e1_1"))

            after_run = await stub.Run(
                engine_pb2.RunRequest(run_id=run_id, max_time=600)
            )

            logging.info("Starting Calibrator at flow rate 0")

            await stub.ApplyActions(
                engine_pb2.ActionBundle(
                    run_id=run_id,
                    actions=[
                        engine_pb2.Action(
                            setter=engine_pb2.GenericSetter(
                                domain="calibrator",
                                setter_name="setFlow",
                                object_id="ca_0",
                                value=Value(number_value=after_run.new_time),
                                additional_parameters=[
                                    engine_pb2.Parameter(
                                        name="end",
                                        value=Value(
                                            number_value=float(
                                                after_run.new_time + 600
                                            )
                                        ),
                                    ),
                                    engine_pb2.Parameter(
                                        name="vehsPerHour",
                                        value=Value(number_value=100),
                                    ),
                                    engine_pb2.Parameter(
                                        name="speed",
                                        value=Value(number_value=10),
                                    ),
                                    engine_pb2.Parameter(
                                        name="typeID",
                                        value=Value(string_value="DEFAULT_VEHTYPE"),
                                    ),
                                    engine_pb2.Parameter(
                                        name="routeID",
                                        value=Value(string_value="f_2"),
                                    ),
                                ],
                            )
                        )
                    ],
                )
            )

            after_run = await stub.Run(
                engine_pb2.RunRequest(run_id=run_id, max_time=600)
            )

            # await stub.ApplyActions(
            #     engine_pb2.ActionBundle(
            #         run_id=run_id,
            #         actions=[
            #             engine_pb2.Action(
            #                 setter=engine_pb2.GenericSetter(
            #                     domain="calibrator",
            #                     setter_name="setFlow",
            #                     object_id="ca_0",
            #                     value=Value(number_value=after_run.new_time),
            #                     additional_parameters=[
            #                         engine_pb2.Parameter(
            #                             name="end",
            #                             value=Value(
            #                                 number_value=float(
            #                                     after_run.new_time + 600
            #                                 )
            #                             ),
            #                         ),
            #                         engine_pb2.Parameter(
            #                             name="vehsPerHour",
            #                             value=Value(number_value=250),
            #                         ),
            #                         engine_pb2.Parameter(
            #                             name="speed",
            #                             value=Value(number_value=-1),
            #                         ),
            #                         engine_pb2.Parameter(
            #                             name="typeID",
            #                             value=Value(string_value="DEFAULT_VEHTYPE"),
            #                         ),
            #                         engine_pb2.Parameter(
            #                             name="routeID",
            #                             value=Value(string_value="f_2"),
            #                         ),
            #                     ],
            #                 )
            #             )
            #         ],
            #     )
            # )
            logging.info("Calibrator should stop by now")

            after_run = await stub.Run(
                engine_pb2.RunRequest(run_id=run_id, max_time=1000)
            )

            await stub.CloseRun(engine_pb2.CloseRunRequest(run_id=run_id))
            controller.cancel()

        # scenario = asyncio.create_task(scenario_stop_random())
        # scenario = asyncio.create_task(scenario_stop_random_async_tls())
        scenario = asyncio.create_task(scenario_service_station())

        try:
            await scenario
        except Exception as e:
            # Exceptions handled in teardown
            errors.append(str(e))
            pass

        await handle_teardown(scenario, streaming)

        for err in errors:
            logging.error(f"Error: {err}")

        for warn in warnings:
            logging.warning(f"Warning: {warn}")

        logging.info(
            f"Execution terminated with {len(errors)} error(s) and {len(warnings)} warning(s)."
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    asyncio.run(main())

    sys.exit(0)
