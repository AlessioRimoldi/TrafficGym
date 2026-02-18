from __future__ import annotations
import asyncio

import grpc

# from .kernel import RunConfig, RunState, Interrupt, InterruptEvent, ValueType
from trafficgym.api import engine_pb2_grpc
from trafficgym.api.engine_pb2 import (
    KeyValue,
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
    ActionBundle,
    ApplyActionsResponse,
    StreamRequest,
    TelemetryFrame,
    SubscribeRequest,
    SubscribeResponse,
    UnsubscribeRequest,
    UnsubscribeResponse,
    RegisterInterruptRequest,
    InterruptEvent as EngineInterruptEvent,
    AcknowledgeInterruptRequest,
    CancelInterruptRequest,
    CancelInterruptResponse,
    FetchRequest,
    FetchResponse,
)

from google.protobuf.struct_pb2 import Value

from trafficgym.engine.helpers import extract_value, ExtractedValueType

from trafficgym.engine.ports.simulation import (
    SimulationPort,
    Interrupt,
    InterruptEvent as SimulationInterruptEvent,
    RunConfig,
)

from trafficgym.engine.ports.adapter_factory import AdapterFactory

from trafficgym.engine.adapters.factories import LibsumoAdapterFactory

# from trafficgym.engine.adapters.fake_adapter import FakeAdapter

# from libsumo import DOMAINS
from typing import Any, Callable, AsyncIterator, Literal

from functools import partial

import logging

import libsumo  # type: ignore


class InvalidGetterError(Exception):
    def __init__(self, message_or_exception: str | Exception):
        if isinstance(message_or_exception, Exception):
            self.original = message_or_exception
            message = str(message_or_exception)
        else:
            message = message_or_exception

        super().__init__(message)


def raise_async_except(
    task: asyncio.Task[Any], context: grpc.ServicerContext
) -> None:
    if task.exception() is not None:
        context.abort(grpc.StatusCode.UNKNOWN, "An Exception Occured")


class Subscription:
    def __init__(
        self,
        # domain: Domain,
        domain: str,
        getter_name: str,
        object_id: str,
        parameters: dict[str, Value],
        name: str | None = None,
    ):
        self.__name = name
        self.domain = domain
        self.getter_name = getter_name
        self.object_id = object_id
        self.parameters = parameters

    @property
    def name(self) -> str:
        """Returns the name of the subscription if set, otherwise the fingerprint"""
        return self.__name or self.fingerprint()

    def fingerprint(self) -> str:
        return f"{str(self.domain)}.{self.getter_name}_{self.parameters}"

    def collect(self, simulation: SimulationPort) -> str:
        if self.getter_name.startswith("_"):
            raise InvalidGetterError(
                "Dunder names are blocked from being collected."
            )
        if not self.getter_name.startswith("get"):
            raise InvalidGetterError(
                "Collection only possible for functions beginning with 'get'."
            )

        string_params: dict[str, str] = {}

        for k, v in self.parameters.items():
            kind = v.WhichOneof("kind")

            if kind == "string_value":
                string_params[k] = v.string_value
            elif kind == "number_value":
                string_params[k] = str(v.number_value)
            else:
                raise InvalidGetterError(
                    f"Unknown type in additional parameters: got {kind} for {k}"
                )

        try:
            collected = simulation.query(
                self.domain,
                self.getter_name,
                self.object_id,
                self.parameters,
            )
            # collected_typed: Value
            # try:
            #     collected_typed = Value(number_value=float(collected))
            # except ValueError:
            #     collected_typed = Value(string_value=collected)

        # except AttributeError as e:
        #     raise InvalidGetterError("Unknown getter") from e
        except Exception as e:
            raise InvalidGetterError("Error executing getter") from e
            # in future, we can check the getter an"d name exist before calling
            # and also send the client the available getters before they exec.

        # return collected_typed
        return collected


class SubscriptionManager:
    subscriptions: dict[str, Subscription]
    metrics: dict[str, list[str | None]]

    def __init__(
        self,
        simulation: SimulationPort,
        subscription_queues: dict[str, asyncio.Queue[TelemetryFrame | None]],
        frame_builder: Callable[[list[KeyValue]], TelemetryFrame],
    ):
        self.simulation = simulation
        self.subscription_queues = subscription_queues

        self.subscriptions = {}
        self.metrics = {}

        self.telemetryStep = 0
        self._frame_builder = frame_builder

    async def collect(self) -> list[tuple[Subscription, Exception]]:
        # self.newMetrics: dict[str, Value | None] = {}
        self.newMetrics: dict[str, str | None] = {}
        failed_collects: list[tuple[Subscription, Exception]] = []
        for subscription in self.subscriptions.values():
            try:
                collected = subscription.collect(self.simulation)
            except InvalidGetterError as e:
                failed_collects.append((subscription, e))
                collected = None
            except Exception as e:
                raise e

            self.newMetrics[subscription.name] = collected

            history = self.metrics.get(subscription.fingerprint()) or []
            history.append(collected)
            self.metrics[subscription.fingerprint()] = history

        return failed_collects

    async def queue_recent_collect(self) -> None:
        q = self.subscription_queues[self.simulation.run_id]
        frame = self._frame_builder(
            [
                KeyValue(key=k, has_value=v is None, value=v or "")
                for k, v in self.newMetrics.items()
            ]
        )
        await q.put(frame)

    def lookup_recent_collection(self, fingerprint: str) -> str | None:
        history = self.metrics.get(fingerprint)
        if not history:
            return None
        else:
            return history[-1]

    def subscribe(
        self,
        # domain: Domain,
        domain: str,
        getter_name: str,
        object_id: str,
        parameters: dict[str, Value] | None = None,
        name: str | None = None,
    ) -> str:
        parameters = parameters or {}
        newSub = Subscription(domain, getter_name, object_id, parameters, name)
        if newSub.fingerprint() in self.subscriptions:
            logging.warning(
                f"Received a request to subscribe to {domain}.{getter_name}"
                f"{parameters}. "
                f"\nThis failed because a subscription is already registered "
                f"with the same domain, getter_name and object_id. The "
                f"corresponding fingerprint is {newSub.fingerprint()}."
            )
            raise Exception("Subscription already exists")

        self.subscriptions[newSub.fingerprint()] = newSub
        return newSub.fingerprint()

    def unsubscribe(self, fingerprint: str) -> None:
        if fingerprint not in self.subscriptions:
            logging.warning(
                f"Received a request to unsubscribe from {fingerprint}, "
                f"but the subscription was not found."
            )
            raise Exception("Unsubscribe failed")

        del self.subscriptions[fingerprint]


class EngineService(engine_pb2_grpc.EngineServiceServicer):
    def __init__(self, adapter_factory: AdapterFactory) -> None:
        self.runs: dict[str, SimulationPort] = {}
        self.telemetry_queues: dict[
            str, asyncio.Queue[TelemetryFrame | None]
        ] = {}
        self.subscription_queues: dict[
            str, asyncio.Queue[TelemetryFrame | None]
        ] = {}
        self.run_tasks: dict[str, asyncio.Task[Any]] = {}
        self._subscription_manager: SubscriptionManager | None = None
        self._adapter_factory = adapter_factory

    def _build_frame(
        self, metrics: list[KeyValue], run_id: str
    ) -> TelemetryFrame:
        if run_id not in self.runs:
            raise RuntimeError("Run ID not found")

        run = self.runs[run_id]

        return TelemetryFrame(
            run_id=run_id,
            step=run.step,
            sim_time_s=run.step * run.seconds_per_step,
            metrics=metrics,
        )

    async def _log_client(
        self, run_id: str, log_type: Literal["Warning", "Error"], msg: str
    ) -> None:
        if run_id not in self.telemetry_queues:
            logging.warning(
                "Failed to send client log message! "
                f"Could not find telemetry queue for run {run_id}."
            )
        frame = self._build_frame(
            run_id=run_id,
            metrics=[KeyValue(key=log_type, has_value=True, value=msg)],
        )

        await self.telemetry_queues[run_id].put(frame)

    async def CreateRun(
        self,
        request: CreateRunRequest,
        context: grpc.ServicerContext,
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
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"Error running SUMO: {e}"
            )

        self.runs[run.run_id] = run
        self.telemetry_queues[run.run_id] = asyncio.Queue()
        self.subscription_queues[run.run_id] = asyncio.Queue()

        part = partial(self._build_frame, run_id=run.run_id)
        self._subscription_manager = SubscriptionManager(
            run, self.subscription_queues, part
        )

        return CreateRunResponse(run_id=run.run_id, input_artifacts=[])

    async def Run(
        self, request: RunRequest, context: grpc.ServicerContext
    ) -> RunResponse:
        logging.debug(f"Run: {request}")
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")

        if run_id in self.run_tasks:
            logging.warning(
                f"Trying to start a run in {run_id}, but this run is already executing."
            )
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Run is already executing, maybe await its end",
            )

        run = self.runs[run_id]

        if run.closed:
            context.abort(
                grpc.StatusCode.ABORTED,
                "Run was closed and can not longer be run",
            )

        run_mode: str | None = request.WhichOneof("run_mode")
        if run_mode == "steps":
            steps = request.steps
        elif run_mode == "time":
            steps = int(request.time * run.steps_per_second)
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
        self.run_tasks[run_id] = task
        await task
        raise_async_except(task, context)
        new_step = run.step
        new_time = new_step * run.seconds_per_step
        return RunResponse(run_id=run_id, new_step=new_step, new_time=new_time)

    async def CloseRun(
        self, request: CloseRunRequest, context: grpc.ServicerContext
    ) -> CloseRunResponse:
        logging.debug(f"CloseRun: {request}")
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        run = self.runs[run_id]

        if run.closed:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Run already closed"
            )

        if run_id in self.run_tasks:
            logging.warning(
                f"Closing Run {run_id}, despite running task for that run"
            )
        run.close()
        await self.telemetry_queues[run_id].put(None)
        await self.subscription_queues[run_id].put(None)
        return CloseRunResponse(run_id=run_id)

    async def ApplyActions(
        self, request: ActionBundle, context: grpc.ServicerContext
    ) -> ApplyActionsResponse:
        # breakpoint()
        logging.debug(f"ApplyAction: {request}")
        application_results: list[KeyValue] = []
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")

        if request.HasField("step"):
            context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                "steps not supported in ApplyActions.",
            )

        run = self.runs[run_id]
        for a in request.actions:
            p: str | None = a.WhichOneof("payload")
            if p == "setter":
                additional_parameters: dict[str, ExtractedValueType] = {
                    param.name: extract_value(param.value)
                    for param in a.setter.parameters
                }
                try:
                    run.apply(
                        a.setter.domain,
                        a.setter.setter_name,
                        a.setter.object_id,
                        {
                            parameter.name: parameter.value
                            for parameter in a.setter.parameters
                        },
                    )
                except (AttributeError, TypeError) as e:
                    logging.warning(
                        f"Received a malformed setter request: {a.setter.domain}."
                        f"{a.setter.parameters})\n{e}"
                    )
                    application_results.append(
                        KeyValue(
                            key="Error",
                            has_value=True,
                            value=f"Setter: {str(e)}",
                        )
                    )
                    await context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "Setter not found or request malformed.",
                    )

                application_results.append(
                    KeyValue(
                        key="Info",
                        has_value=True,
                        value=f"Setter Called {a.setter.domain}.{a.setter.setter_name}"
                        f"{a.setter.parameters})",
                    )
                )

        frame = self._build_frame(run_id=run_id, metrics=application_results)
        await self.telemetry_queues[run_id].put(frame)
        return ApplyActionsResponse(run_id=run_id)

    async def StreamTelemetry(
        self, request: StreamRequest, context: grpc.ServicerContext
    ) -> AsyncIterator[TelemetryFrame]:
        logging.debug(f"StreamTelemetry: {request}")
        run_id = request.run_id
        if run_id not in self.telemetry_queues:
            context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
            return
        q = self.telemetry_queues[run_id]
        while True:
            frame = await q.get()
            if frame is None:
                return
            yield frame

    async def StreamSubscriptions(
        self, request: StreamRequest, context: grpc.ServicerContext
    ) -> AsyncIterator[TelemetryFrame]:
        logging.debug(f"StreeamSubscription: {request}")
        run_id = request.run_id
        if run_id not in self.subscription_queues:
            context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
            return

        q = self.subscription_queues[run_id]
        while True:
            frame = await q.get()
            if frame is None:
                return
            yield frame

    async def Subscribe(
        self,
        request: SubscribeRequest,
        context: grpc.ServicerContext,
    ) -> SubscribeResponse:
        if self._subscription_manager is None:
            context.abort(
                grpc.StatusCode.ABORTED, "Subscription Manager not initialised."
            )
            return
        logging.debug(f"Subscribe: {request}")
        additional_parameters = {p.name: p.value for p in request.parameters}

        if request.name in map(
            lambda x: x.name, self._subscription_manager.subscriptions.values()
        ):
            logging.warning(
                f"Duplicate subscription name "
                f"{request.name}. Rejected request."
            )
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                f"Subscription with name {request.name} already exists.",
            )

        fingerprint = self._subscription_manager.subscribe(
            request.domain,
            request.getter_name,
            request.object_id,
            {
                parameter.name: parameter.value
                for parameter in request.parameters
            },
            request.name,
        )

        return SubscribeResponse(fingerprint=fingerprint)

    async def Unsubscribe(
        self,
        request: UnsubscribeRequest,
        context: grpc.ServicerContext,
    ) -> UnsubscribeResponse:
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, "unsubscription not implemented"
        )

    async def RegisterInterrupt(
        self,
        request: RegisterInterruptRequest,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[EngineInterruptEvent]:
        """CAUTION: Comparaison value must use the same subfield as the
        collected value, or the interrupt will never trigger
        TODO Improve this!"""
        run_id = request.run_id
        if run_id not in self.runs:
            yield context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"run ${run_id} not found."
            )
        run = self.runs[run_id]

        interrupt_requests: asyncio.Queue[SimulationInterruptEvent | None] = (
            asyncio.Queue()
        )

        new_interrupt = Interrupt(
            trigger_metric_name=request.trigger_metric.name,
            trigger_metric_value=request.trigger_metric.value,
            trigger_metric_op=request.trigger_metric.op,
            interrupt_requests=interrupt_requests,
        )

        run.interrupts[new_interrupt.interrupt_id] = new_interrupt
        logging.debug(
            f"Registered new Interrupt {new_interrupt.trigger_metric_name}_{new_interrupt.trigger_metric_value}"
        )

        while True:
            frame = await interrupt_requests.get()
            if frame is None:
                return
            # new_interrupt.active_interrupt_event = InterruptEvent(
            #     frame.observed_value
            # )
            yield EngineInterruptEvent(
                interrupt_id=new_interrupt.interrupt_id,
                event_id=frame.event_id,
                observed_value=frame.observed_value,
            )
            logging.debug("Issued Interrupt Event")

    async def AcknowledgeInterrupt(
        self,
        request: AcknowledgeInterruptRequest,
        context: grpc.ServicerContext,
    ) -> ApplyActionsResponse:
        run_id = request.run_id
        if run_id not in self.runs:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"run ${run_id} not found."
            )
        run = self.runs[run_id]

        if request.interrupt_id not in run.interrupts:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Could not find interrupt {request.interrupt_id}",
            )

        acknowledged_interrupt = run.interrupts[request.interrupt_id]

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
                        context=context,
                    )

                else:
                    new_name = request.new_interrupt_conditions.name
                    new_value = request.new_interrupt_conditions.value

                    if new_name != "":
                        acknowledged_interrupt.trigger_metric_name = new_name
                        logging.debug(
                            f"Updated interrupt {acknowledged_interrupt.interrupt_id} "
                            f"trigger name to {new_name}."
                        )
                    if request.new_interrupt_conditions.HasField("value"):
                        acknowledged_interrupt.trigger_metric_value = new_value
                        logging.debug(
                            f"Updated interrupt {acknowledged_interrupt.interrupt_id} "
                            f"trigger value to {str(extract_value(new_value))}"
                        )

                apply_actions_response = await self.ApplyActions(
                    request.actions, context
                )
                acknowledged_interrupt.interrupt_requests.task_done()
                logging.debug("Interrupt Acknoledge Received and Processed")
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
        context: grpc.ServicerContext,
    ) -> CancelInterruptResponse:

        run_id = request.run_id
        if run_id not in self.runs:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"run ${run_id} not found."
            )
        run = self.runs[run_id]

        removed_interrupt = run.interrupts[request.interrupt_id]
        del run.interrupts[request.interrupt_id]

        if removed_interrupt:
            await removed_interrupt.interrupt_requests.put(None)
            logging.debug(
                f"Cancelled interrupt {removed_interrupt.trigger_metric_name} "
                f"triggers at {removed_interrupt.trigger_metric_value}"
            )
            removed_interrupt.interrupt_requests.task_done()
            return CancelInterruptResponse(
                interrupt_id=removed_interrupt.interrupt_id
            )

        await context.abort(
            grpc.StatusCode.NOT_FOUND,
            f"Interrupt with id {request.interrupt_id} not found.",
        )

    async def FetchSubscription(
        self, request: FetchRequest, context: grpc.ServicerContext
    ) -> FetchResponse:
        if self._subscription_manager is None:
            await context.abort(
                grpc.StatusCode.ABORTED, "Subscription manager not initialised."
            )

        if request.requires_collect:
            await self._subscription_manager.collect()

        collected = self._subscription_manager.lookup_recent_collection(
            request.fingerprint
        )

        if collected is None:
            return FetchResponse(
                fetched=KeyValue(
                    key=request.fingerprint, has_value=False, value=""
                )
            )

        return FetchResponse(
            fetched=KeyValue(
                key=request.fingerprint, has_value=True, value=collected
            )
        )

    async def _run_loop(self, run_id: str, steps: int) -> None:
        run = self.runs[run_id]
        q = self.telemetry_queues[run_id]

        if self._subscription_manager is None:
            raise RuntimeError("Subscription manager not initialised.")

        try:
            # run.start(max_steps=max_steps) # run already started in create run
            for _ in range(steps):
                _, _, metrics = run.tick()

                failed_getters_and_exceptions: list[
                    tuple[Subscription, Exception]
                ] = []

                try:
                    failed_getters_and_exceptions = (
                        await self._subscription_manager.collect()
                    )
                    await self._subscription_manager.queue_recent_collect()

                    # decision for now to run interrupts
                    # after collecting subscriptions.
                    # list() to copy dictionary state,
                    # during handling, new interrupts may be registered

                    # logging.info(f"{len(run.interrupts)} interrupts registered")
                    for i in list(run.interrupts.values()):
                        if i.active_interrupt_event:
                            continue

                        recent_collection_str = (
                            self._subscription_manager.lookup_recent_collection(
                                fingerprint=i.trigger_metric_name
                            )
                        )

                        if not recent_collection_str:
                            message = (
                                "Interrupt trigger check collection "
                                f"for {i.trigger_metric_name} failed."
                            )
                            logging.warning(message)
                            asyncio.create_task(
                                self._log_client(run.run_id, "Error", message)
                            )
                            continue

                        trigger_value_kind = i.trigger_metric_value.WhichOneof(
                            "kind"
                        )

                        # if collection_type != trigger_type:
                        #     message = (
                        #         f"Interrupt trigger {i.trigger_metric_name} "
                        #         f"Value type ({trigger_type}) does not "
                        #         f"match collected ({collection_type})"
                        #     )
                        #     logging.warning(message)
                        #     asyncio.create_task(
                        #         self._log_client(run.run_id, "Error", message)
                        #     )
                        #     continue

                        recent_collection: float | str
                        trigger: float | str

                        triggered = False

                        if trigger_value_kind == "number_value":
                            recent_collection = float(recent_collection_str)
                            recent_collection_value = Value(
                                number_value=recent_collection
                            )
                            trigger = i.trigger_metric_value.number_value

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
                            recent_collection_value = Value(
                                string_value=recent_collection_str
                            )

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
                                observed_value=recent_collection_value
                            )
                            i.active_interrupt_event = event
                            await i.interrupt_requests.put(event)
                            logging.debug("Enqueing interrupt request")

                            # need to wait for interrupts to be processed before continuing
                            try:
                                logging.debug(
                                    f"Awiting Interrupt processing! {i.trigger_metric_name} val {i.trigger_metric_value}"
                                )
                                await asyncio.wait_for(
                                    i.interrupt_requests.join(), 1
                                )
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
                            KeyValue(
                                key=f"Error",
                                has_value=True,
                                value=f"Failed to collect for {sub.name}: "
                                f"{exception}: {exception.__cause__}",
                            )
                        )
                        logging.warning(
                            f"Subscription collection failed for {sub.name}"
                        )

                    frame = self._build_frame(
                        run_id=run_id,
                        metrics=errors,
                    )
                    await q.put(frame)

                frame = self._build_frame(
                    run_id=run_id,
                    metrics=[
                        KeyValue(
                            key=k, has_value=True, value=str(extract_value(v))
                        )
                        for k, v in metrics.items()
                    ],
                )
                await q.put(frame)
                await asyncio.sleep(0)
        except Exception as e:
            logging.error(e.__str__())
            raise e
        finally:
            self.run_tasks.pop(run_id)
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
