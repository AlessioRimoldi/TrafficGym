from trafficgym.api import engine_pb2_grpc, engine_pb2
from typing import (
    AsyncIterable,
    Literal,
    AsyncIterator,
    TypeVar,
    ParamSpec,
    Callable,
    Awaitable,
)
from trafficgym.experiment_sdk import sdk
from functools import wraps
from contextlib import asynccontextmanager
# from celery.utils.log import get_task_logger

import logging
import grpc.aio
import uuid


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
log = logging.getLogger("rpc")
log.propagate = False


def log_rpc(
    rpc_call_id: uuid.UUID,
    rpc_name: str,
    direction: Literal["REQUEST", "RESPONSE"],
    payload: object,
) -> None:
    log.info(
        f"{rpc_name} {direction} ({rpc_call_id})",
        extra={
            "log_type": "rpc",
            "request_or_response": direction,
            "rpc_name": rpc_name,
            "rpc_call_id": rpc_call_id,
            "payload": payload,
        },
    )


P = ParamSpec("P")
R = TypeVar("R")


def _grpc_error_handler(
    fn: Callable[P, Awaitable[R]],
) -> Callable[P, Awaitable[R]]:
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
        signal_id: str,
        state: str,
    ) -> sdk.ApplyActionsResponse:
        set_trafficlight_action = sdk.Action(
            "trafficlight",
            "setRedYellowGreenState",
            signal_id,
            [("state", sdk.Value(state))],
        )
        return await self.run.apply_actions(
            sdk.ActionBundle([set_trafficlight_action])
        )


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

        response = sdk.CreateRunResponse.from_proto(await self.stub.CreateRun(request.to_proto()))

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

    async def __aenter__(self) -> RunHandle:
        return self

    async def __aexit__(self) -> None:
        await self.close_run()

    @_grpc_error_handler
    async def run_max_steps(self, max_steps: int) -> sdk.RunResponse:
        request = sdk.RunRequest(self.run_id, ("max_steps", max_steps))

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "run_max_steps", "REQUEST", request.to_dict())

        response = sdk.RunResponse.from_proto(await self.driver.stub.Run(request.to_proto()))

        log_rpc(rpc_call_id, "run_max_steps", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def run_max_time(self, max_time: float) -> sdk.RunResponse:
        request = sdk.RunRequest(run_id=self.run_id, run_mode=("max_time", max_time))

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "run_max_time", "REQUEST", request.to_dict())

        response = sdk.RunResponse.from_proto(await self.driver.stub.Run(request.to_proto()))

        log_rpc(rpc_call_id, "run_max_time", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def run_steps(self, steps: int) -> sdk.RunResponse:
        request = sdk.RunRequest(self.run_id, ("steps", steps))

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "run_steps", "REQUEST", request.to_dict())

        response = sdk.RunResponse.from_proto(await self.driver.stub.Run(request.to_proto()))

        log_rpc(rpc_call_id, "run_steps", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def run_time(self, seconds: float) -> sdk.RunResponse:
        request = sdk.RunRequest(self.run_id, ("time", seconds))

        rpc_call_id = uuid.uuid4()
        log_rpc(rpc_call_id, "run_time", "RESPONSE", request.to_dict())

        response = sdk.RunResponse.from_proto(await self.driver.stub.Run(request.to_proto()))

        log_rpc(rpc_call_id, "run_time", "RESPONSE", response.to_dict())

        return response

    @_grpc_error_handler
    async def close_run(self) -> None:
        if self._closed:
            return

        await self.driver.stub.CloseRun(
            engine_pb2.CloseRunRequest(run_id=self.run_id)
        )

        self._closed = True

    @_grpc_error_handler
    async def apply_actions(
        self, action_bundle: sdk.ActionBundle
    ) -> sdk.ApplyActionsResponse:
        request = engine_pb2.ApplyActionsRequest(
            run_id=self.run_id, action_bundle=action_bundle.to_proto()
        )

        rpc_call_id = uuid.uuid4()
        log_rpc(
            rpc_call_id,
            "apply_actions",
            "REQUEST",
            sdk.ApplyActionsRequest.from_proto(request).to_dict(),
        )

        response = await self.driver.stub.ApplyActions(request)

        log_rpc(
            rpc_call_id,
            "apply_actions",
            "RESPONSE",
            sdk.ApplyActionsResponse.from_proto(response).to_dict(),
        )

        return sdk.ApplyActionsResponse.from_proto(response)

    def _convert_frame(
        self, frame_pb: engine_pb2.TelemetryFrame
    ) -> sdk.TelemetryFrame:
        return sdk.TelemetryFrame(
            frame_pb.run_id,
            frame_pb.step,
            frame_pb.sim_time_s,
            [sdk.KeyValue.from_proto(m) for m in frame_pb.metrics],
        )

    @_grpc_stream_error_handler
    async def stream_telemetry(self) -> AsyncIterator[sdk.TelemetryFrame]:
        async for frame in self.driver.stub.StreamTelemetry(
            engine_pb2.StreamRequest(run_id=self.run_id)
        ):
            yield self._convert_frame(frame)

    @_grpc_stream_error_handler
    async def stream_subscriptions(self) -> AsyncIterator[sdk.TelemetryFrame]:
        async for frame in self.driver.stub.StreamSubscriptions(
            engine_pb2.StreamRequest(run_id=self.run_id)
        ):
            yield self._convert_frame(frame)

    @_grpc_error_handler
    async def subscribe(
        self,
        domain: str,
        getter_name: str,
        object_id: str,
        parameters: list[tuple[str, sdk.Value]] | None = None,
        name: str | None = None,
    ) -> sdk.SubscriptionResponse:
        if parameters is None:
            parameters = []

        response = await self.driver.stub.Subscribe(
            engine_pb2.SubscribeRequest(
                run_id=self.run_id,
                domain=domain,
                getter_name=getter_name,
                object_id=object_id,
                parameters=[
                    engine_pb2.Parameter(name=p[0], value=p[1].to_proto())
                    for p in parameters
                ],
                name=name,
            )
        )

        return sdk.SubscriptionResponse.from_proto(response)

    @_grpc_error_handler
    async def unsubscribe(self) -> None:
        raise NotImplementedError("Unsubscribe not implemented")

    @_grpc_stream_error_handler
    async def register_interrupt(
        self,
        trigger_conditions: sdk.TriggerConditions,
    ) -> AsyncIterator[sdk.InterruptEvent]:

        request = engine_pb2.RegisterInterruptRequest(
            run_id=self.run_id, trigger_metric=trigger_conditions.to_proto()
        )

        async for interrupt_event in self.driver.stub.RegisterInterrupt(
            request
        ):
            yield sdk.InterruptEvent(
                interrupt_event.run_id,
                interrupt_event.interrupt_id,
                interrupt_event.event_id,
                interrupt_event.observed_value,
            )

    @_grpc_error_handler
    async def acknowledge_interrupt(
        self,
        interrupt_id: str,
        event_id: str,
        action_bundle: sdk.ActionBundle,
        new_trigger_conditions: sdk.TriggerConditions,
    ) -> sdk.ApplyActionsResponse:
        response = await self.driver.stub.AcknowledgeInterrupt(
            engine_pb2.AcknowledgeInterruptRequest(
                run_id=self.run_id,
                interrupt_id=interrupt_id,
                event_id=event_id,
                actions=action_bundle.to_proto(),
                new_interrupt_conditions=new_trigger_conditions.to_proto(),
            )
        )

        return sdk.ApplyActionsResponse.from_proto(response)

    @_grpc_error_handler
    async def cancel_interrupt(self, interrupt_id: str) -> None:
        await self.driver.stub.CancelInterrupt(
            engine_pb2.CancelInterruptRequest(
                run_id=self.run_id, interrupt_id=interrupt_id
            )
        )

    @_grpc_error_handler
    async def fetch_subscription(
        self,
        fingerprint: str,
        immediate_collect: bool,
    ) -> sdk.KeyValue:
        response = await self.driver.stub.FetchSubscription(
            engine_pb2.FetchRequest(
                run_id=self.run_id,
                fingerprint=fingerprint,
                requires_collect=immediate_collect,
            )
        )

        return sdk.KeyValue.from_proto(response.fetched)
