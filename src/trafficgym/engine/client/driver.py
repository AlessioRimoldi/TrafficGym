from trafficgym.api import engine_pb2_grpc, engine_pb2
from typing import (
    AsyncIterable,
    Literal,
    AsyncIterator,
    TypeVar,
    ParamSpec,
    Callable,
    Any,
    Coroutine,
)
from trafficgym.experiment_sdk import sdk
from functools import wraps
from contextlib import asynccontextmanager
from enum import Enum, auto

# from celery.utils.log import get_task_logger

import asyncio
import logging
import grpc.aio
import uuid

InterruptStream = AsyncIterable[sdk.InterruptEvent | None]


def _handle_rpc_error(err: grpc.aio.AioRpcError) -> Exception:
    code = err.code()
    details = err.details()

    if code == grpc.StatusCode.INVALID_ARGUMENT:
        return sdk.InvalidArgumentError(details)
    elif code == grpc.StatusCode.NOT_FOUND:
        return sdk.NotFoundError(details)
    elif code == grpc.StatusCode.ABORTED:
        return sdk.AbortedError(details)
    elif code == grpc.StatusCode.UNAVAILABLE:
        return sdk.ServiceUnavailableError(details)
    elif code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return TimeoutError(details)
    else:
        return sdk.GrpcError(details)


# log = get_task_logger(__name__)
rpc_logger = logging.getLogger("rpc")
rpc_logger.propagate = False


def log_rpc(
    rpc_call_id: uuid.UUID,
    rpc_name: str,
    direction: Literal["REQUEST", "RESPONSE", "OTHER"],
    payload: object,
) -> None:
    rpc_logger.info(
        f"{rpc_name} {direction} ({rpc_call_id})",
        extra={
            "log_type": "rpc",
            "direction": direction,
            "rpc_name": rpc_name,
            "rpc_call_id": rpc_call_id,
            "payload": payload,
        },
    )


subscription_logger = logging.getLogger("subscription")
subscription_logger.propagate = False
telemetry_logger = logging.getLogger("telemetry")
telemetry_logger.propagate = False

P = ParamSpec("P")
R = TypeVar("R")


def _grpc_error_handler(
    fn: Callable[P, Coroutine[Any, Any, R]],
) -> Callable[P, Coroutine[Any, Any, R]]:
    @wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await fn(*args, **kwargs)
        except grpc.aio.AioRpcError as e:
            raise _handle_rpc_error(e) from e

    return wrapper


T = TypeVar("T")


def _grpc_stream_error_handler(
    fn: Callable[P, AsyncIterable[T]],
) -> Callable[P, AsyncIterator[T]]:
    @wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> AsyncIterator[T]:
        try:
            async for item in fn(*args, **kwargs):
                yield item
        except grpc.aio.AioRpcError as e:
            raise _handle_rpc_error(e) from e

    return wrapper


class TrafficLightController:
    def __init__(self, run: RunHandle) -> None:
        self.run = run

    @_grpc_error_handler
    async def set_signal(
        self,
        tls_id: str,
        state: str,
    ) -> sdk.ApplyActionsResponse:

        log_rpc(
            uuid.uuid4(),
            "set_signal",
            "OTHER",
            {"tls_id": tls_id, "state": state},
        )

        set_trafficlight_action = sdk.Action.one_param_action(
            "trafficlight", "setRedYellowGreenState", "state", state, tls_id
        )

        return await self.run.apply_actions(
            sdk.ActionBundle([set_trafficlight_action])
        )

    @_grpc_error_handler
    async def set_program(
        self, tls_id: str, program_id: str
    ) -> sdk.ApplyActionsResponse:
        log_rpc(
            uuid.uuid4(),
            "set_program",
            "OTHER",
            {"tls_id": tls_id, "programID": program_id},
        )

        set_program_action = sdk.Action.one_param_action(
            "trafficlight", "setProgram", "programID", program_id, tls_id
        )

        return await self.run.apply_actions(
            sdk.ActionBundle([set_program_action])
        )

    @_grpc_error_handler
    async def static_tls_controller(
        self,
        signal_id: str,
        phases: list[str],
        durations: list[int],
        initial_step: int,
    ) -> asyncio.Task[None]:
        """Run through the phases provided as strings.
        Once the corresponding duration has elapsed since
        the signal changed, switch to the next phase."""
        assert len(phases) == len(durations)

        await self.set_signal(signal_id, phases[0])

        get_time_subscription = await self.run.subscribe(
            "simulation", "getTime", None
        )
        # Will not work if multiple TLS programs are running as
        # double subscription to time will raise an exception

        time_subscription_name = get_time_subscription.fingerprint

        interrupt_registration = await self.run.register_interrupt(
            sdk.TriggerConditions(
                time_subscription_name,
                sdk.Value(durations[0] + initial_step),
                sdk.Operation.GEQ,
            )
        )

        interrupt_event_stream = self.run.stream_interrupts(
            interrupt_registration.interrupt_id
        )
        logging.debug("TLS Program interrupt registration sent")

        async def tls_program_async() -> None:
            i = 0
            async for interrupt_event in interrupt_event_stream:
                if not interrupt_event:
                    return
                i += 1  # start at 1 because we have already initialised state 0

                try:
                    observed_value = float(interrupt_event.observed_value)
                except:
                    message = "Failed to read interrupt observed value"
                    logging.warning(message)
                    observed_value = 0.0

                await self.run.acknowledge_interrupt(
                    interrupt_event.interrupt_id,
                    interrupt_event.event_id,
                    sdk.ActionBundle(
                        [
                            sdk.Action(
                                "trafficlight",
                                "setRedYellowGreenState",
                                signal_id,
                                [
                                    sdk.Parameter(
                                        "state",
                                        sdk.Value(phases[i % len(phases)]),
                                    )
                                ],
                            )
                        ]
                    ),
                    sdk.TriggerConditions(
                        time_subscription_name,
                        sdk.Value(
                            float(durations[i % len(phases)] + observed_value)
                        ),
                        sdk.Operation.GEQ,
                    ),
                )

        return asyncio.create_task(tls_program_async(), name="tls_program")

    async def _wait_for_interrupt(
        self,
        stream: InterruptStream,
    ) -> sdk.InterruptEvent | None:
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
                await self.run.cancel_interrupt(interrupt_id)

        return None

    @_grpc_error_handler
    async def meter_controller(
        self, tls_id: str, det_id: str
    ) -> asyncio.Task[None]:
        async def gen_traffic_interrupt(
            trigger: float, operation: sdk.Operation
        ) -> InterruptStream:
            interrupt_registration = await self.run.register_interrupt(
                sdk.TriggerConditions(
                    detector_nearside.fingerprint,
                    sdk.Value(trigger),
                    operation,
                )
            )

            return self.run.stream_interrupts(
                interrupt_registration.interrupt_id
            )

        async def acknowledge_interrupt(
            interrupt_event: sdk.InterruptEvent, new_program_id: str
        ) -> None:
            await self.run.acknowledge_interrupt(
                interrupt_event.interrupt_id,
                interrupt_event.event_id,
                sdk.ActionBundle(
                    [
                        sdk.Action.one_param_action(
                            "trafficlight",
                            "setProgram",
                            "programID",
                            new_program_id,
                            tls_id,
                        )
                    ]
                ),
                None,
            )

        detector_nearside = await self.run.subscribe(
            "inductionloop", "getLastIntervalOccupancy", det_id
        )

        await self.run.apply_actions(
            sdk.ActionBundle(
                [
                    sdk.Action.one_param_action(
                        "trafficlight", "setProgram", "programID", "0", tls_id
                    ),
                ]
            )
        )

        interrupt_event: sdk.InterruptEvent | None = None

        async def meter_controller_async() -> None:
            class MeterState(Enum):
                OFF = auto()
                QUARTER = auto()
                TENTH = auto()
                CHOKE = auto()

            meter_state: MeterState = MeterState.OFF

            while True:
                if meter_state is MeterState.OFF:
                    high_traffic_interrupts = gen_traffic_interrupt(
                        15.0, sdk.Operation.GEQ
                    )

                    interrupt_event = None

                    async for event in await high_traffic_interrupts:
                        if not event:
                            return
                        interrupt_event = event
                        logging.info("Setting meter to 1/4 mode")
                        await acknowledge_interrupt(interrupt_event, "1")
                        meter_state = MeterState.QUARTER
                        break

                elif meter_state is MeterState.QUARTER:
                    high_traffic_interrupts = gen_traffic_interrupt(
                        20.0, sdk.Operation.GEQ
                    )
                    low_traffic_interrupts = gen_traffic_interrupt(
                        10.0, sdk.Operation.LEQ
                    )

                    high_traffic_task = asyncio.create_task(
                        self._wait_for_interrupt(await high_traffic_interrupts)
                    )
                    low_traffic_task = asyncio.create_task(
                        self._wait_for_interrupt(await low_traffic_interrupts)
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
                        await acknowledge_interrupt(interrupt_event, "2")
                        meter_state = MeterState.TENTH

                    else:
                        logging.info("Setting meter OFF")
                        await acknowledge_interrupt(interrupt_event, "off")
                        meter_state = MeterState.OFF

                elif meter_state is MeterState.TENTH:
                    high_traffic_interrupts = gen_traffic_interrupt(
                        30.0, sdk.Operation.GEQ
                    )
                    low_traffic_interrupts = gen_traffic_interrupt(
                        15.0, sdk.Operation.LEQ
                    )

                    interrupt_event = None

                    high_traffic_task = asyncio.create_task(
                        self._wait_for_interrupt(await high_traffic_interrupts)
                    )
                    low_traffic_task = asyncio.create_task(
                        self._wait_for_interrupt(await low_traffic_interrupts)
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
                        await acknowledge_interrupt(interrupt_event, "3")
                        meter_state = MeterState.CHOKE

                    else:
                        logging.info("Setting meter to 1/4 mode")
                        await acknowledge_interrupt(interrupt_event, "1")
                        meter_state = MeterState.QUARTER

                elif meter_state is MeterState.CHOKE:
                    low_traffic_interrupts = gen_traffic_interrupt(
                        25.0, sdk.Operation.LEQ
                    )

                    interrupt_event = None

                    async for event in await low_traffic_interrupts:
                        if not event:
                            return
                        interrupt_event = event
                        logging.info("Setting meter to 1/10 mode")
                        await acknowledge_interrupt(interrupt_event, "2")
                        meter_state = MeterState.TENTH
                        break

                else:
                    logging.warning("Unknown meter state, resetting to OFF")
                    meter_state = MeterState.OFF
                    await self.run.tls.set_program(tls_id, "0")

        return asyncio.create_task(meter_controller_async())


class EngineDriver:
    def __init__(self, stub: engine_pb2_grpc.EngineServiceAsyncStub) -> None:
        self.stub = stub

    @asynccontextmanager
    @_grpc_stream_error_handler
    async def create_run(
        self,
        sumocfg_path: str,
        sumo_binary: Literal["sumo", "sumo-gui"],
        step_length_ms: int,
    ) -> AsyncIterator[RunHandle]:
        request = sdk.CreateRunRequest(
            sumocfg_path=sumocfg_path,
            sumo_binary=sumo_binary,
            step_length_ms=step_length_ms,
        )

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "create_run", "REQUEST", request.to_dict())

        response = sdk.CreateRunResponse.from_proto(
            await self.stub.CreateRun(request.to_proto())
        )

        log_rpc(rpc_call_id, "create_run", "RESPONSE", response.to_dict())

        handle = RunHandle(self, response.run_id)

        try:
            yield handle
        finally:
            await handle.close_run()


class RunHandle:
    def __init__(self, driver: EngineDriver, run_id: str) -> None:
        self._closed = False
        self.driver = driver
        self.run_id = run_id
        self.tls = TrafficLightController(self)

        self._subscription_task: asyncio.Task[None] = asyncio.create_task(
            self._handle_subscriptions()
        )
        self._telemetry_task: asyncio.Task[None] = asyncio.create_task(
            self._handle_telemetry()
        )

    async def __aenter__(self) -> RunHandle:
        return self

    async def __aexit__(self) -> None:
        await self.close_run()

    @_grpc_error_handler
    async def run_max_steps(self, max_steps: int) -> sdk.RunResponse:
        request = sdk.RunRequest(self.run_id, ("max_steps", max_steps))

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "run_max_steps", "REQUEST", request.to_dict())

        response = sdk.RunResponse.from_proto(
            await self.driver.stub.Run(request.to_proto())
        )

        log_rpc(rpc_call_id, "run_max_steps", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def run_max_time(self, max_time: float) -> sdk.RunResponse:
        request = sdk.RunRequest(
            run_id=self.run_id, run_mode=("max_time", max_time)
        )

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "run_max_time", "REQUEST", request.to_dict())

        response = sdk.RunResponse.from_proto(
            await self.driver.stub.Run(request.to_proto())
        )

        log_rpc(rpc_call_id, "run_max_time", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def run_steps(self, steps: int) -> sdk.RunResponse:
        request = sdk.RunRequest(self.run_id, ("steps", steps))

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "run_steps", "REQUEST", request.to_dict())

        response = sdk.RunResponse.from_proto(
            await self.driver.stub.Run(request.to_proto())
        )

        log_rpc(rpc_call_id, "run_steps", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def run_time(self, seconds: float) -> sdk.RunResponse:
        request = sdk.RunRequest(self.run_id, ("time", seconds))

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "run_time", "REQUEST", request.to_dict())

        response = sdk.RunResponse.from_proto(
            await self.driver.stub.Run(request.to_proto())
        )

        log_rpc(rpc_call_id, "run_time", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def close_run(self) -> sdk.CloseRunResponse | None:
        if self._closed:
            return None

        request = sdk.CloseRunRequest(self.run_id)

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "close_run", "REQUEST", request.to_dict())

        response = sdk.CloseRunResponse.from_proto(
            await self.driver.stub.CloseRun(request.to_proto())
        )

        log_rpc(rpc_call_id, "close_run", "RESPONSE", response.to_dict())

        self._closed = True

        await asyncio.gather(
            self._subscription_task,
            self._telemetry_task,
            return_exceptions=False,
        )

        return response

    @_grpc_error_handler
    async def apply_actions(
        self, action_bundle: sdk.ActionBundle
    ) -> sdk.ApplyActionsResponse:
        request = sdk.ApplyActionsRequest(self.run_id, action_bundle)

        rpc_call_id = uuid.uuid4()
        log_rpc(
            rpc_call_id,
            "apply_actions",
            "REQUEST",
            request.to_dict(),
        )

        response = sdk.ApplyActionsResponse.from_proto(
            await self.driver.stub.ApplyActions(request.to_proto())
        )

        log_rpc(
            rpc_call_id,
            "apply_actions",
            "RESPONSE",
            response.to_dict(),
        )

        return response

    def _convert_frame(
        self, frame_pb: engine_pb2.TelemetryFrame
    ) -> sdk.TelemetryFrame:
        return sdk.TelemetryFrame(
            frame_pb.run_id,
            frame_pb.step,
            frame_pb.sim_time_s,
            [sdk.KeyValue.from_proto(m) for m in frame_pb.metrics],
        )

    @_grpc_error_handler
    async def _handle_telemetry(self) -> None:
        request = sdk.StreamRequest(self.run_id)

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "stream_telemetry", "REQUEST", request.to_dict())

        async for frame in self.driver.stub.StreamTelemetry(request.to_proto()):
            if frame is None:
                return

            sdk_frame = self._convert_frame(frame)
            for metric in sdk_frame.metrics:
                telemetry_logger.info(
                    "Telemetry data received",
                    extra={
                        "simulation_time": sdk_frame.sim_time_s,
                        "simulation_step": sdk_frame.step,
                        "telemetry_name": metric.key,
                        "payload": (
                            metric.value.to_dict()
                            if metric.value is not None
                            else ""
                        ),
                    },
                )

    @_grpc_error_handler
    async def _handle_subscriptions(self) -> None:
        request = sdk.StreamRequest(self.run_id)

        log_rpc(
            uuid.uuid4(), "stream_subscriptions", "REQUEST", request.to_dict()
        )

        async for frame in self.driver.stub.StreamSubscriptions(
            request.to_proto()
        ):
            if frame is None:
                return

            sdk_frame = self._convert_frame(frame)

            for metric in sdk_frame.metrics:
                subscription_logger.info(
                    "Subscription data received",
                    extra={
                        "simulation_time": sdk_frame.sim_time_s,
                        "simulation_step": sdk_frame.step,
                        "subscription_fingerprint": metric.key,
                        "payload": (
                            metric.value.to_dict()["value"]
                            if metric.value is not None
                            else ""
                        ),
                    },
                )

    @_grpc_error_handler
    async def subscribe(
        self,
        domain: str,
        getter_name: str,
        object_id: str | None,
        parameters: list[sdk.Parameter] | None = None,
        name: str | None = None,
    ) -> sdk.SubscriptionResponse:
        if parameters is None:
            parameters = []

        request = sdk.SubscriptionRequest(
            self.run_id, domain, getter_name, object_id, parameters
        )

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "subscribe", "REQUEST", request.to_dict())

        response = sdk.SubscriptionResponse.from_proto(
            await self.driver.stub.Subscribe(request.to_proto())
        )

        log_rpc(rpc_call_id, "subscribe", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def unsubscribe(self) -> None:
        raise NotImplementedError("Unsubscribe not implemented")

    @_grpc_error_handler
    async def register_interrupt(
        self,
        trigger_conditions: sdk.TriggerConditions,
    ) -> sdk.RegisterInterruptResponse:
        request = sdk.RegisterInterruptRequest(
            run_id=self.run_id, trigger_metric=trigger_conditions
        )

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "register_interrupt", "REQUEST", request.to_dict())

        response = sdk.RegisterInterruptResponse.from_proto(
            await self.driver.stub.RegisterInterrupt(request.to_proto())
        )

        log_rpc(
            rpc_call_id, "register_interrupt", "RESPONSE", response.to_dict()
        )

        return response

    @_grpc_stream_error_handler
    async def stream_interrupts(
        self, interrupt_id: str
    ) -> AsyncIterator[sdk.InterruptEvent]:
        request = sdk.StreamInterruptsRequest(self.run_id, interrupt_id)

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "stream_interrupts", "REQUEST", request.to_dict())

        async for interrupt_event in self.driver.stub.StreamInterrupts(
            request.to_proto()
        ):
            sdk_event = sdk.InterruptEvent(
                interrupt_event.run_id,
                interrupt_event.interrupt_id,
                interrupt_event.event_id,
                interrupt_event.observed_value,
            )

            log_rpc(
                rpc_call_id,
                "stream_interrupts",
                "RESPONSE",
                sdk_event.to_dict(),
            )

            yield sdk_event

    @_grpc_error_handler
    async def acknowledge_interrupt(
        self,
        interrupt_id: str,
        event_id: str,
        action_bundle: sdk.ActionBundle,
        new_trigger_conditions: sdk.TriggerConditions | None,
    ) -> sdk.ApplyActionsResponse:
        request = sdk.AcknowledgeInterruptRequest(
            self.run_id,
            interrupt_id,
            event_id,
            action_bundle,
            new_trigger_conditions,
        )

        rpc_call_id = uuid.uuid4()
        log_rpc(
            rpc_call_id, "acknowledge_interrupt", "REQUEST", request.to_dict()
        )

        response = sdk.ApplyActionsResponse.from_proto(
            await self.driver.stub.AcknowledgeInterrupt(
                engine_pb2.AcknowledgeInterruptRequest(
                    run_id=self.run_id,
                    interrupt_id=interrupt_id,
                    event_id=event_id,
                    actions=action_bundle.to_proto(),
                    new_interrupt_conditions=(
                        new_trigger_conditions.to_proto()
                        if new_trigger_conditions is not None
                        else None
                    ),
                )
            )
        )

        log_rpc(
            rpc_call_id, "acknowledge_interrupt", "RESPONSE", response.to_dict()
        )

        return response

    @_grpc_error_handler
    async def cancel_interrupt(self, interrupt_id: str) -> None:
        request = sdk.CancelInterruptRequest(self.run_id, interrupt_id)

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "cancel_interrupt", "REQUEST", request.to_dict())

        response = sdk.CancelInterruptResponse.from_proto(
            await self.driver.stub.CancelInterrupt(request.to_proto())
        )

        log_rpc(rpc_call_id, "cancel_interrupt", "RESPONSE", response.to_dict())

    @_grpc_error_handler
    async def fetch_subscription(
        self,
        fingerprint: str,
        immediate_collect: bool,
    ) -> sdk.KeyValue:
        request = sdk.FetchRequest(self.run_id, fingerprint, immediate_collect)

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "fetch_subscription", "REQUEST", request.to_dict())

        response = sdk.FetchResponse.from_proto(
            await self.driver.stub.FetchSubscription(request.to_proto())
        )

        log_rpc(
            rpc_call_id, "fetch_subscription", "RESPONSE", response.to_dict()
        )

        return response.fetched
