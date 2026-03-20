from __future__ import annotations
import asyncio

import grpc.aio

# from .kernel import RunConfig, RunState, Interrupt, InterruptEvent, ValueType
from trafficgym.api import engine_pb2_grpc
from trafficgym.api.engine_pb2 import (
    CustomValue,
    NamedNullableString,
    EQU,
    NEQ,
    GRT,
    LST,
    LEQ,
    GEQ,
    CreateRunRequest,
    CreateRunResponse,
    RunRequest,
    RunResponse,
    CloseRunRequest,
    CloseRunResponse,
    ApplyActionsRequest,
    ApplyActionsResponse,
    StreamRequest,
    TelemetryFrame,
    SubscribeRequest,
    SubscribeResponse,
    UnsubscribeRequest,
    UnsubscribeResponse,
    RegisterInterruptRequest,
    RegisterInterruptResponse,
    StreamInterruptsRequest,
    InterruptEvent as EngineInterruptEvent,
    AcknowledgeInterruptRequest,
    CancelInterruptRequest,
    CancelInterruptResponse,
    FetchRequest,
    FetchResponse,
)

from trafficgym.engine.helpers import extract_value

from trafficgym.engine.ports.simulation import (
    SimulationPort,
    Interrupt,
    InterruptEvent as SimulationInterruptEvent,
    RunConfig,
    InvalidGetterError,
)

from trafficgym.engine.ports.adapter_factory import AdapterFactory

from trafficgym.engine.adapters.factories import LibsumoAdapterFactory

from dataclasses import dataclass

# from trafficgym.engine.adapters.fake_adapter import FakeAdapter

# from libsumo import DOMAINS
from typing import cast, Callable, AsyncIterator, Literal

from functools import partial

import logging

import libsumo  # type: ignore


class Subscription:
    def __init__(
        self,
        # domain: Domain,
        domain: str,
        getter_name: str,
        object_id: str | None,
        parameters: dict[str, CustomValue],
    ):
        self._domain = domain
        self._getter_name = getter_name
        self._object_id = object_id
        self._parameters = parameters

    # @property
    # def name(self) -> str:
    #     """Returns the name of the subscription if set, otherwise the fingerprint"""
    #     # return self.__name or self.fingerprint()
    #     return self.fingerprint()

    # TODO HAVE PROPER HASH, FINGERPRINT UNRELIABLE BUT USER READABLE

    @property
    def fingerprint(self) -> str:
        param_str = ",".join(
            f"{k}={extract_value(v)}"
            for k, v in sorted(self._parameters.items())
        )
        return f"{str(self._domain)}.{self._object_id}.{self._getter_name}({param_str})"

    def collect(self, simulation: SimulationPort) -> str:
        try:
            collected = simulation.query(
                self._domain,
                self._getter_name,
                self._object_id,
                self._parameters,
            )

        except Exception as e:
            raise InvalidGetterError("Error executing getter") from e
            # in future, we can check the getter an"d name exist before calling
            # and also send the client the available getters before they exec.

        return collected


class SubscriptionInvalidGetterError(Exception):
    pass


class SubscriptionNameCollisionError(Exception):
    pass


class SubscriptionFingerprintCollisionError(Exception):
    pass


class SubscriptionManager:
    subscriptions: dict[str, Subscription]
    metrics: dict[str, list[str | None]]

    def __init__(
        self,
        simulation: SimulationPort,
        subscription_queue: asyncio.Queue[TelemetryFrame | None],
        frame_builder: Callable[[list[NamedNullableString]], TelemetryFrame],
    ):
        self.simulation = simulation
        self.subscription_queue = subscription_queue

        self.subscriptions = {}
        self.metrics = {}

        self.telemetryStep = 0
        self._frame_builder = frame_builder

    def check_subscription(self, fingerprint: str) -> bool:
        return fingerprint in self.subscriptions

    async def collect(self) -> list[tuple[Subscription, Exception]]:
        self.newMetrics: dict[str, str | None] = {}
        failed_collects: list[tuple[Subscription, Exception]] = []
        for subscription in self.subscriptions.values():
            try:
                collected = subscription.collect(self.simulation)
            except InvalidGetterError as e:
                failed_collects.append((subscription, e))
                collected = None

            self.newMetrics[subscription.fingerprint] = collected

            history = self.metrics.get(subscription.fingerprint) or []
            history.append(collected)
            self.metrics[subscription.fingerprint] = history

        return failed_collects

    async def queue_recent_collect(self) -> None:
        q = self.subscription_queue
        frame = self._frame_builder(
            [
                NamedNullableString(
                    name=k, has_value=v is not None, value=v or ""
                )
                for k, v in self.newMetrics.items()
            ]
        )
        await q.put(frame)

    def lookup_recent_collection(self, fingerprint: str) -> str | None:
        history = self.metrics.get(fingerprint)
        if history is None:
            return None
        else:
            return history[-1]

    def subscribe(
        self,
        domain: str,
        getter_name: str,
        object_id: str | None,  # Will always be str here
        parameters: dict[str, CustomValue] | None = None,
    ) -> str:
        parameters = parameters or {}
        if object_id == "":
            object_id = None

        if getter_name.startswith("_"):
            raise SubscriptionInvalidGetterError(
                "Getter names starting with '_' are blocked from being collected.",
            )

        if not getter_name.startswith("get"):
            raise SubscriptionInvalidGetterError(
                "Collection only possible for functions beginning with 'get'.",
            )

        newSub = Subscription(domain, getter_name, object_id, parameters)
        if newSub.fingerprint in self.subscriptions:
            raise SubscriptionFingerprintCollisionError(
                f"Received a request to subscribe to {domain}.{getter_name}"
                f"{parameters}. "
                f"This failed because a subscription is already registered "
                f"with the same domain, getter_name and object_id. The "
                f"corresponding fingerprint is {newSub.fingerprint}."
            )

        self.subscriptions[newSub.fingerprint] = newSub
        return newSub.fingerprint

    # def unsubscribe(self, fingerprint: str) -> None:
    #     if fingerprint not in self.subscriptions:
    #         logging.warning(
    #             f"Received a request to unsubscribe from {fingerprint}, "
    #             f"but the subscription was not found."
    #         )
    #         raise Exception("Unsubscribe failed")

    #     del self.subscriptions[fingerprint]


@dataclass
class RunContext:
    run: SimulationPort
    subscription_manager: SubscriptionManager
    telemetry_queue: asyncio.Queue[TelemetryFrame | None]
    subscription_queue: asyncio.Queue[TelemetryFrame | None]
    task: asyncio.Task[None] | None


class EngineService(engine_pb2_grpc.EngineServiceServicer):
    def __init__(self, adapter_factory: AdapterFactory) -> None:
        self._runs: dict[str, RunContext] = {}
        self._adapter_factory = adapter_factory

    def _build_frame(
        self, metrics: list[NamedNullableString], run_id: str
    ) -> TelemetryFrame:
        if run_id not in self._runs:
            raise RuntimeError("Run ID not found")

        run_context = self._runs[run_id]

        return TelemetryFrame(
            run_id=run_id,
            step=run_context.run.step,
            sim_time_s=run_context.run.step * run_context.run.seconds_per_step,
            metrics=metrics,
        )

    async def _log_client(
        self, run_id: str, log_type: Literal["Warning", "Error"], msg: str
    ) -> None:
        if run_id not in self._runs:
            logging.warning(
                "Failed to send client log message! "
                f"Could not find telemetry queue for run {run_id}."
            )

        frame = self._build_frame(
            run_id=run_id,
            metrics=[
                NamedNullableString(name=log_type, has_value=True, value=msg)
            ],
        )

        await self._runs[run_id].telemetry_queue.put(frame)

    async def CreateRun(
        self,
        request: CreateRunRequest,
        context: grpc.aio.ServicerContext[CreateRunRequest, CreateRunResponse],
    ) -> CreateRunResponse:
        logging.debug(f"CreateRun: {request}")
        cfg = RunConfig(
            sumocfg_path=request.sumocfg_path,
            sumo_binary=request.sumo_binary or "sumo",
        )
        step_length_ms = (
            request.step_length_ms
            if request.step_length_ms is not None
            else 1000
        )
        run = self._adapter_factory.create(cfg, step_length_ms)

        # need to start now so that we can setup simulation
        # before vehicles start moving (tls etc...)
        try:
            run.start()
        except libsumo.TraCIException as e:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"Error running SUMO: {e}"
            )

        telemetry_queue: asyncio.Queue[TelemetryFrame | None] = asyncio.Queue()
        subscription_queue: asyncio.Queue[TelemetryFrame | None] = (
            asyncio.Queue()
        )

        part = partial(self._build_frame, run_id=run.run_id)
        subscription_manager = SubscriptionManager(
            run, subscription_queue, part
        )

        new_run_context = RunContext(
            run, subscription_manager, telemetry_queue, subscription_queue, None
        )

        self._runs[run.run_id] = new_run_context

        return CreateRunResponse(run_id=run.run_id)

    async def Run(
        self,
        request: RunRequest,
        context: grpc.aio.ServicerContext[RunRequest, RunResponse],
    ) -> RunResponse:
        logging.debug(f"Run: {request}")
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        runContext = self._runs[run_id]

        if runContext.task is not None:
            logging.warning(
                f"Trying to start a run in {run_id}, but this run is already executing."
            )
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Run is already executing, maybe await its end",
            )

        if runContext.run.closed:
            await context.abort(
                grpc.StatusCode.ABORTED,
                "Run was closed and can not longer be run",
            )

        run_mode: str | None = request.WhichOneof("run_mode")
        if run_mode == "steps":
            steps = request.steps
        elif run_mode == "time":
            steps = int(request.time * runContext.run.steps_per_second)
        elif run_mode == "max_steps":
            # steps = request.max_steps - run.step
            await context.abort(
                grpc.StatusCode.UNIMPLEMENTED, "max_step unimplemented"
            )
        elif run_mode == "max_time":
            # max_steps = int(
            #     1000 * request.max_time / run.cfg.step_length_ms
            # )
            # steps = max_steps - run.step
            await context.abort(
                grpc.StatusCode.UNIMPLEMENTED, "max_time unimplemented"
            )
        else:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Run Mode not specified."
            )

        task = asyncio.create_task(self._run_loop(run_id, steps))
        runContext.task = task

        try:
            await task
        except Exception:
            await context.abort(
                grpc.StatusCode.UNKNOWN, "An exception occurred."
            )

        new_step = runContext.run.step
        new_time = new_step * runContext.run.seconds_per_step
        return RunResponse(run_id=run_id, new_step=new_step, new_time=new_time)

    async def CloseRun(
        self,
        request: CloseRunRequest,
        context: grpc.aio.ServicerContext[CloseRunRequest, CloseRunResponse],
    ) -> CloseRunResponse:
        logging.debug(f"CloseRun: {request}")
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        runContext = self._runs[run_id]

        if runContext.run.closed:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Run already closed"
            )

        if runContext.task is not None:
            logging.warning(
                f"Closing Run {run_id}, despite running task for that run"
            )

        runContext.run.close()
        await runContext.telemetry_queue.put(None)
        await runContext.subscription_queue.put(None)
        return CloseRunResponse(run_id=run_id)

    async def ApplyActions(
        self,
        request: ApplyActionsRequest,
        context: grpc.aio.ServicerContext[
            ApplyActionsRequest, ApplyActionsResponse
        ],
    ) -> ApplyActionsResponse:
        logging.debug(f"ApplyAction: {request}")
        application_errors: list[NamedNullableString] = []
        application_info: list[NamedNullableString] = []
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        if request.action_bundle.HasField("step"):
            await context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                "steps not supported in ApplyActions.",
            )

        runContext = self._runs[run_id]
        for a in request.action_bundle.actions:
            p: str | None = a.WhichOneof("payload")
            if p == "setter":
                try:
                    runContext.run.apply(
                        a.setter.domain,
                        a.setter.setter_name,
                        a.setter.object_id,
                        {
                            parameter.name: parameter.value
                            for parameter in a.setter.parameters
                        },
                    )
                except (AttributeError, TypeError, libsumo.TraCIException) as e:
                    logging.warning(
                        f"Received a malformed setter request: {a.setter.domain}."
                        f"{str(a.setter.parameters).replace("\n", "").replace(" ", "")}\n{e}"
                    )
                    application_errors.append(
                        NamedNullableString(
                            name="Error",
                            has_value=True,
                            value=f"Setter: {str(e)}",
                        )
                    )
                    # if we abort here, then next actions in bundle won't be applied
                    # await await context.abort(
                    #     grpc.StatusCode.INVALID_ARGUMENT,
                    #     "Setter not found or request malformed.",
                    # )

                application_info.append(
                    NamedNullableString(
                        name="Info",
                        has_value=True,
                        value=f"Setter Called {a.setter.domain}.{a.setter.setter_name}"
                        f"{a.setter.parameters})",
                    )
                )

        if len(application_errors) + len(application_info) > 0:
            frame = self._build_frame(
                run_id=run_id, metrics=application_info + application_errors
            )
            await runContext.telemetry_queue.put(frame)
        return ApplyActionsResponse(
            errors=[kv.value for kv in application_errors]
        )

    async def StreamTelemetry(
        self,
        request: StreamRequest,
        context: grpc.aio.ServicerContext[
            StreamRequest, AsyncIterator[TelemetryFrame]
        ],
    ) -> AsyncIterator[TelemetryFrame]:
        logging.debug(f"StreamTelemetry: {request}")
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        runContext = self._runs[run_id]

        q = runContext.telemetry_queue
        while True:
            frame = await q.get()
            if frame is None:
                return
            yield frame

    async def StreamSubscriptions(
        self,
        request: StreamRequest,
        context: grpc.aio.ServicerContext[
            StreamRequest, AsyncIterator[TelemetryFrame]
        ],
    ) -> AsyncIterator[TelemetryFrame]:
        logging.debug(f"StreamSubscription: {request}")
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        runContext = self._runs[run_id]

        q = runContext.subscription_queue
        while True:
            frame = await q.get()
            if frame is None:
                return
            yield frame

    async def Subscribe(
        self,
        request: SubscribeRequest,
        context: grpc.aio.ServicerContext[SubscribeRequest, SubscribeResponse],
    ) -> SubscribeResponse:
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        runContext = self._runs[run_id]

        try:
            fingerprint = runContext.subscription_manager.subscribe(
                request.domain,
                request.getter_name,
                request.object_id,
                {
                    parameter.name: parameter.value
                    for parameter in request.parameters
                },
            )
            logging.debug(f"Subscribe: {request}")

        except SubscriptionInvalidGetterError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except (
            SubscriptionNameCollisionError,
            SubscriptionFingerprintCollisionError,
        ) as e:
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, str(e))

        return SubscribeResponse(fingerprint=fingerprint)

    async def Unsubscribe(
        self,
        request: UnsubscribeRequest,
        context: grpc.aio.ServicerContext[
            UnsubscribeRequest, UnsubscribeResponse
        ],
    ) -> UnsubscribeResponse:
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, "unsubscription not implemented"
        )

    async def RegisterInterrupt(
        self,
        request: RegisterInterruptRequest,
        context: grpc.aio.ServicerContext[
            RegisterInterruptRequest, RegisterInterruptResponse
        ],
    ) -> RegisterInterruptResponse:
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        run_context = self._runs[run_id]

        if not run_context.subscription_manager.check_subscription(
            request.trigger_metric.subscription_fingerprint
        ):
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "The interrupt metric must first be subscribed to.",
            )

        interrupt_requests: asyncio.Queue[SimulationInterruptEvent | None] = (
            asyncio.Queue()
        )

        new_interrupt = Interrupt(
            trigger_metric_fingerprint=request.trigger_metric.subscription_fingerprint,
            trigger_metric_value=request.trigger_metric.value,
            trigger_metric_op=request.trigger_metric.op,
            interrupt_requests=interrupt_requests,
        )

        run_context.run.interrupts[new_interrupt.interrupt_id] = new_interrupt
        logging.debug(
            f"Registered new Interrupt {new_interrupt.trigger_metric_fingerprint}_{new_interrupt.trigger_metric_value}"
        )

        return RegisterInterruptResponse(
            interrupt_id=new_interrupt.interrupt_id
        )

    async def StreamInterrupts(
        self,
        request: StreamInterruptsRequest,
        context: grpc.aio.ServicerContext[
            StreamInterruptsRequest, AsyncIterator[EngineInterruptEvent]
        ],
    ) -> AsyncIterator[EngineInterruptEvent]:
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        run_context = self._runs[run_id]

        if request.interrupt_id not in run_context.run.interrupts:
            logging.warning(
                f"Could not find interrupt '{request.interrupt_id}'."
            )
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"interrupt_id '{request.interrupt_id}' not found.",
            )

        interrupt_requests = run_context.run.interrupts[
            request.interrupt_id
        ].interrupt_requests

        while True:
            frame = await interrupt_requests.get()
            if frame is None:
                return

            try:
                yield EngineInterruptEvent(
                    run_id=run_id,
                    interrupt_id=request.interrupt_id,
                    event_id=frame.event_id,
                    observed_value=str(extract_value(frame.observed_value)),
                )
                logging.debug("Issued Interrupt Event")
            finally:
                interrupt_requests.task_done()

    async def AcknowledgeInterrupt(
        self,
        request: AcknowledgeInterruptRequest,
        context: grpc.aio.ServicerContext[
            AcknowledgeInterruptRequest, ApplyActionsResponse
        ],
    ) -> ApplyActionsResponse:
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        runContext = self._runs[run_id]

        if request.interrupt_id not in runContext.run.interrupts:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Could not find interrupt {request.interrupt_id}",
            )

        acknowledged_interrupt = runContext.run.interrupts[request.interrupt_id]

        if not acknowledged_interrupt.active_interrupt_event:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Interrupt {acknowledged_interrupt.interrupt_id} "
                f"is not interrupted",
            )
        elif (
            acknowledged_interrupt.active_interrupt_event.event_id
            != request.event_id
        ):
            expected_event_id = (
                acknowledged_interrupt.active_interrupt_event.event_id
            )
            await context.abort(
                grpc.StatusCode.ABORTED,
                "The acknowledgment is for the wrong interrupt event! "
                f"Expected {expected_event_id}, but got "
                f"{request.event_id}",
            )
        else:
            try:
                if not request.HasField("new_interrupt_conditions"):
                    await self.CancelInterrupt(
                        request=CancelInterruptRequest(
                            run_id=run_id, interrupt_id=request.interrupt_id
                        ),
                        context=cast(
                            grpc.aio.ServicerContext[
                                CancelInterruptRequest, CancelInterruptResponse
                            ],
                            context,
                        ),
                    )

                else:
                    new_fingerprint = (
                        request.new_interrupt_conditions.subscription_fingerprint
                    )
                    new_value = request.new_interrupt_conditions.value

                    if new_fingerprint != "":
                        if not runContext.subscription_manager.check_subscription(
                            new_fingerprint
                        ):
                            await context.abort(
                                grpc.StatusCode.FAILED_PRECONDITION,
                                "The interrupt metric must first be subscribed to.",
                            )

                        acknowledged_interrupt.trigger_metric_fingerprint = (
                            new_fingerprint
                        )
                        logging.debug(
                            f"Updated interrupt {acknowledged_interrupt.interrupt_id} "
                            f"subscription metric to {new_fingerprint}."
                        )
                    if request.new_interrupt_conditions.HasField("value"):
                        acknowledged_interrupt.trigger_metric_value = new_value
                        logging.debug(
                            f"Updated interrupt {acknowledged_interrupt.interrupt_id} "
                            f"trigger value to {str(extract_value(new_value))}"
                        )

                apply_actions_response = await self.ApplyActions(
                    ApplyActionsRequest(
                        run_id=run_id, action_bundle=request.actions
                    ),
                    context=cast(
                        grpc.aio.ServicerContext[
                            ApplyActionsRequest, ApplyActionsResponse
                        ],
                        context,
                    ),
                )
                acknowledged_interrupt.active_interrupt_event.ack.set()
                logging.debug("Interrupt Acknowledge Received and Processed")
                acknowledged_interrupt.active_interrupt_event = None

                return apply_actions_response
                # logging.info(f"Interrupt action would occur here: {request.actions}")
                # return ApplyActionsResponse()
            except Exception as e:
                logging.error(str(e))
                await context.abort(
                    grpc.StatusCode.ABORTED, "Error executing interrupt."
                )

    async def CancelInterrupt(
        self,
        request: CancelInterruptRequest,
        context: grpc.aio.ServicerContext[
            CancelInterruptRequest, CancelInterruptResponse
        ],
    ) -> CancelInterruptResponse:
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        runContext = self._runs[run_id]

        if request.interrupt_id not in runContext.run.interrupts:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Interrupt with id {request.interrupt_id} not found.",
            )

        removed_interrupt = runContext.run.interrupts[request.interrupt_id]
        del runContext.run.interrupts[request.interrupt_id]

        await removed_interrupt.interrupt_requests.put(None)
        logging.debug(
            f"Cancelled interrupt {removed_interrupt.trigger_metric_fingerprint} "
            f"triggers at {removed_interrupt.trigger_metric_value}"
        )
        removed_interrupt.interrupt_requests.task_done()
        return CancelInterruptResponse(
            interrupt_id=removed_interrupt.interrupt_id
        )

    async def FetchSubscription(
        self,
        request: FetchRequest,
        context: grpc.aio.ServicerContext[FetchRequest, FetchResponse],
    ) -> FetchResponse:
        run_id = request.run_id
        if run_id not in self._runs:
            logging.warning(f"Could not find run '{run_id}'.")
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"run_id '{run_id}' not found."
            )

        runContext = self._runs[run_id]

        if request.requires_collect:
            try:
                await runContext.subscription_manager.collect()
            except Exception as e:
                await context.abort(grpc.StatusCode.ABORTED, str(e))

        collected = runContext.subscription_manager.lookup_recent_collection(
            request.fingerprint
        )

        if collected is None:
            return FetchResponse(
                fetched=NamedNullableString(
                    name=request.fingerprint, has_value=False, value=""
                )
            )

        return FetchResponse(
            fetched=NamedNullableString(
                name=request.fingerprint, has_value=True, value=collected
            )
        )

    async def _run_loop(self, run_id: str, steps: int) -> None:
        runContext = self._runs[run_id]

        try:
            for _ in range(steps):
                _, _, metrics = runContext.run.tick()

                failed_getters_and_exceptions: list[
                    tuple[Subscription, Exception]
                ] = []

                try:
                    failed_getters_and_exceptions = (
                        await runContext.subscription_manager.collect()
                    )
                    await runContext.subscription_manager.queue_recent_collect()

                    # decision for now to run interrupts
                    # after collecting subscriptions.
                    # list() to copy dictionary state,
                    # during handling, new interrupts may be registered

                    # logging.info(f"{len(run.interrupts)} interrupts registered")
                    for i in list(runContext.run.interrupts.values()):
                        if i.active_interrupt_event:
                            continue

                        recent_collection_str = runContext.subscription_manager.lookup_recent_collection(
                            fingerprint=i.trigger_metric_fingerprint
                        )

                        if not recent_collection_str:
                            message = (
                                "Interrupt trigger check collection "
                                f"for {i.trigger_metric_fingerprint} failed."
                            )
                            logging.warning(message)
                            asyncio.create_task(
                                self._log_client(
                                    runContext.run.run_id, "Error", message
                                )
                            )
                            continue

                        trigger_value_kind = i.trigger_metric_value.WhichOneof(
                            "kind"
                        )

                        recent_collection: float | str
                        trigger: float | str

                        triggered = False

                        if (
                            trigger_value_kind == "float_value"
                            or trigger_value_kind == "int_value"
                        ):
                            recent_collection = float(recent_collection_str)
                            trigger = (
                                i.trigger_metric_value.int_value
                                if trigger_value_kind == "int_value"
                                else i.trigger_metric_value.float_value
                            )

                            if i.trigger_metric_op == EQU:
                                triggered = recent_collection == trigger
                            elif i.trigger_metric_op == NEQ:
                                triggered = recent_collection != trigger
                            elif i.trigger_metric_op == GRT:
                                triggered = recent_collection > trigger
                            elif i.trigger_metric_op == LST:
                                triggered = recent_collection < trigger
                            elif i.trigger_metric_op == GEQ:
                                triggered = recent_collection >= trigger
                            elif i.trigger_metric_op == LEQ:
                                triggered = recent_collection <= trigger
                            else:
                                message = "Unsupported operation for interrupt trigger."
                                logging.warning(message)
                                asyncio.create_task(
                                    self._log_client(run_id, "Error", message)
                                )
                                continue

                        elif trigger_value_kind == "string_value":
                            trigger = i.trigger_metric_value.string_value

                            if i.trigger_metric_op == EQU:
                                triggered = recent_collection_str == trigger
                            elif i.trigger_metric_op == NEQ:
                                triggered = recent_collection_str != trigger
                            else:
                                message = "Unsupported operation for interrupt trigger."
                                logging.warning(message)
                                asyncio.create_task(
                                    self._log_client(run_id, "Error", message)
                                )
                                continue

                        else:
                            message = (
                                "Unsupported interrupt trigger value "
                                "or collection value type"
                            )
                            logging.warning(message)
                            asyncio.create_task(
                                self._log_client(run_id, "Error", message)
                            )
                            continue

                        if triggered:
                            logging.debug("Interrupt triggered")
                            event = SimulationInterruptEvent(
                                observed_value=i.trigger_metric_value,
                            )
                            i.active_interrupt_event = event
                            await i.interrupt_requests.put(event)
                            logging.debug("Enqueing interrupt request")

                            # need to wait for interrupts to be processed before continuing
                            try:
                                logging.debug(
                                    f"Awiting Interrupt processing! {i.trigger_metric_fingerprint} val {i.trigger_metric_value}"
                                )
                                await asyncio.wait_for(event.ack.wait(), 1)
                                logging.debug("Interrupts done processing.")
                            except TimeoutError:
                                message = "Interrupt execution timeout!"
                                logging.warning(message)
                                asyncio.create_task(
                                    self._log_client(run_id, "Error", message)
                                )

                except Exception as e:
                    message = f"Failed to collect subscribed metrics: {str(e)}"
                    logging.warning(message)
                    asyncio.create_task(
                        self._log_client(run_id, "Error", message)
                    )

                if len(failed_getters_and_exceptions) > 0:
                    errors = []
                    for sub, exception in failed_getters_and_exceptions:
                        errors.append(
                            NamedNullableString(
                                name=f"Error",
                                has_value=True,
                                value=f"Failed to collect for {sub.fingerprint}: "
                                f"{exception}: {exception.__cause__}",
                            )
                        )
                        logging.warning(
                            f"Subscription collection failed for {sub.fingerprint}"
                        )

                    frame = self._build_frame(
                        run_id=run_id,
                        metrics=errors,
                    )
                    await runContext.telemetry_queue.put(frame)

                frame = self._build_frame(
                    run_id=run_id,
                    metrics=[
                        NamedNullableString(
                            name=k, has_value=True, value=str(extract_value(v))
                        )
                        for k, v in metrics.items()
                    ],
                )
                await runContext.telemetry_queue.put(frame)
                await asyncio.sleep(0)
        except Exception as e:
            logging.error(e.__str__())
            raise e
        finally:
            runContext.task = None
            # run.close()
            # await q.put(None)


async def serve(host: str = "127.0.0.1", port: int = 50051) -> None:
    server = grpc.aio.server()
    engine_pb2_grpc.add_EngineServiceServicer_to_server(
        EngineService(LibsumoAdapterFactory()), server
    )
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    await server.wait_for_termination()


def main() -> None:
    logging.basicConfig(level=logging.DEBUG)

    asyncio.run(serve(), debug=False)


if __name__ == "__main__":
    main()

# async def handle_interrupt(next_interrupt_event: SimulationInterruptEvent, actions=ActionBundle | None, new_trigger=MetricNameAndValue | None) -> ApplyActionsResponse:
#     interrupt_event = await next_interrupt_event

#     interrupt_acknowledge_request = AcknowledgeInterruptRequest(
#         run_id=create_run_response.run_id,
#         interrupt_id=interrupt_event.interrupt_id,
#         event_id=interrupt_event.event_id,
#         actions=action_bundle_factory(
#             create_run_response.run_id,
#             None,
#             [
#                 GenericSetterType(
#                     "fake_domain",
#                     "setProgram",
#                     "fake_object",
#                     [
#                         Parameter(
#                             name="value", value=Value(string_value="off")
#                         )
#                     ],
#                 )
#             ],
#         ),
#     )

#     await service.AcknowledgeInterrupt(
#         interrupt_acknowledge_request, fake_context
#     )
