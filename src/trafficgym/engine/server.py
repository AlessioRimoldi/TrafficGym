from __future__ import annotations
import asyncio

import grpc

from .kernel import RunConfig, RunState, Interrupt, InterruptEvent
from ..api import engine_pb2, engine_pb2_grpc
from google.protobuf.struct_pb2 import Value

# from libsumo import DOMAINS
from typing import Any, Callable, AsyncIterator

from functools import partial

import logging


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


# Domain = Enum("Domain", list(map(lambda x: x.__name__, DOMAINS)))


class Subscription:
    def __init__(
        self,
        # domain: Domain,
        domain: str,
        getter_name: str,
        object_id: str,
        additional_parameters: dict[str, Any],
        name: str | None = None,
    ):
        self.__name = name
        self.domain = domain
        self.getter_name: str = getter_name
        self.object_id = object_id
        self.additional_parameters = additional_parameters

    @property
    def name(self) -> str:
        """Returns the name of the subscription if set, otherwise the fingerprint"""
        return self.__name or self.fingerprint()

    def fingerprint(self) -> str:
        return f"{str(self.domain)}.{self.getter_name}_{self.object_id}"

    def collect(self, run_state: RunState) -> str:
        if self.getter_name.startswith("_"):
            raise InvalidGetterError(
                "Dunder names are blocked from being collected."
            )
        if not self.getter_name.startswith("get"):
            raise InvalidGetterError(
                "Collection only possible for functions beginning with 'get'."
            )

        try:
            collected = run_state.collect_metric(
                self.domain,
                self.getter_name,
                self.object_id,
                self.additional_parameters,
            )
        except AttributeError as e:
            raise InvalidGetterError("Unknown getter") from e
            # in future, we can check the getter and name exist before calling
            # and also send the client the available getters before they exec.

        return collected


class SubscriptionManager:
    subscriptions: dict[str, Subscription]
    metrics: dict[str, list[str | None]]

    def __init__(
        self,
        run_state: RunState,
        subscription_queues: dict[
            str, asyncio.Queue[engine_pb2.TelemetryFrame | None]
        ],
        frame_builder: Callable[
            [list[engine_pb2.KeyValue]], engine_pb2.TelemetryFrame
        ],
    ):
        self.run_state = run_state
        self.subscription_queues = subscription_queues

        self.subscriptions = {}
        self.metrics = {}

        self.telemetryStep = 0
        self._frame_builder = frame_builder

    async def collect(self) -> list[tuple[Subscription, Exception]]:
        self.newMetrics = {}
        failed_collects: list[tuple[Subscription, Exception]] = []
        for subscription in self.subscriptions.values():
            try:
                collected = subscription.collect(self.run_state)
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
        q = self.subscription_queues[self.run_state.run_id]
        frame = self._frame_builder(
            [
                engine_pb2.KeyValue(key=k, value=Value(string_value=str(v)))
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
        additional_params: dict[str, str] | None = None,
        name: str | None = None,
    ) -> str:
        if not additional_params:
            additional_params = {}
        newSub = Subscription(
            domain, getter_name, object_id, additional_params, name
        )
        if newSub.fingerprint() in self.subscriptions:
            obj_message = ("_" + object_id) if object_id is not None else ""
            param_message = (
                f"with additional parameters {additional_params}"
                if additional_params is not None
                else ""
            )

            logging.warning(
                f"Received a request to subscribe to {domain}.{getter_name}"
                f"{obj_message}{param_message}. "
                f"\nThis failed because a subscription is already registered "
                f"with the same domain, getter_name and object_id. The "
                f"corresponding fingerprint is {newSub.fingerprint()}."
            )
            raise Exception("Subscription already exists")

        self.subscriptions[newSub.fingerprint()] = newSub
        return newSub.name

    def unsubscribe(self, fingerprint: str) -> None:
        if fingerprint not in self.subscriptions:
            logging.warning(
                f"Received a request to unsubscribe from {fingerprint}, "
                f"but the subscription was not found."
            )
            raise Exception("Unsubscribe failed")

        del self.subscriptions[fingerprint]


class EngineService(engine_pb2_grpc.EngineServiceServicer):
    def __init__(self) -> None:
        self.runs: dict[str, RunState] = {}
        self.telemetry_queues: dict[
            str, asyncio.Queue[engine_pb2.TelemetryFrame | None]
        ] = {}
        self.subscription_queues: dict[
            str, asyncio.Queue[engine_pb2.TelemetryFrame | None]
        ] = {}
        self.run_tasks: dict[str, asyncio.Task[Any]] = {}
        self.subscription_manager: SubscriptionManager | None = None

    def _build_frame(
        self, metrics: list[engine_pb2.KeyValue], run_id: str
    ) -> engine_pb2.TelemetryFrame:
        if run_id not in self.runs:
            raise RuntimeError("Run ID not found")

        run = self.runs[run_id]

        return engine_pb2.TelemetryFrame(
            run_id=run_id,
            step=run.step,
            sim_time_s=run.step / run.cfg.step_length_ms,
            metrics=metrics,
        )

    async def CreateRun(
        self,
        request: engine_pb2.CreateRunRequest,
        context: grpc.ServicerContext,
    ) -> engine_pb2.CreateRunResponse:
        logging.debug(f"CreateRun: {request}")
        cfg = RunConfig(
            sumocfg_path=request.sumocfg_path,
            sumo_binary=request.sumo_binary or "sumo",
            step_length_ms=request.step_length_ms or 1000,
        )
        run = RunState(cfg)
        self.runs[run.run_id] = run
        self.telemetry_queues[run.run_id] = asyncio.Queue()
        self.subscription_queues[run.run_id] = asyncio.Queue()

        part = partial(self._build_frame, run_id=run.run_id)
        self.subscription_manager = SubscriptionManager(
            run, self.subscription_queues, part
        )

        # need to start now so that we can setup simulation
        # before vehicles start moving (tls etc...)
        run.start()

        return engine_pb2.CreateRunResponse(
            run_id=run.run_id, input_artifacts=[]
        )

    async def Run(
        self, request: engine_pb2.RunRequest, context: grpc.ServicerContext
    ) -> engine_pb2.RunResponse:
        logging.debug(f"Run: {request}")
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
            return

        if run_id not in self.run_tasks:
            max_run: str | None = request.WhichOneof("max_run")
            if max_run == "max_steps":
                max_steps = request.max_steps
            elif max_run == "max_time":
                max_steps = int(
                    1000
                    * request.max_time
                    / self.runs[run_id].cfg.step_length_ms
                )
            else:
                max_steps = int(
                    1000 * 1000 / self.runs[run_id].cfg.step_length_ms
                )

            task = asyncio.create_task(self._run_loop(run_id, max_steps))
            self.run_tasks[run_id] = task
            await task
            raise_async_except(task, context)
        return engine_pb2.RunResponse(
            run_id=run_id, new_step=self.runs[run_id].step
        )

    async def CloseRun(
        self, request: engine_pb2.CloseRunRequest, context: grpc.ServicerContext
    ) -> engine_pb2.CloseRunResponse:
        logging.debug(f"CloseRun: {request}")
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
            return
        run = self.runs[run_id]

        if run_id in self.run_tasks:
            logging.warning(
                f"Closing Run {run_id}, despite running task for that run"
            )
        run.close()
        run.started = False
        await self.telemetry_queues[run_id].put(None)
        await self.subscription_queues[run_id].put(None)
        return engine_pb2.CloseRunResponse(run_id=run_id)

    async def ApplyActions(
        self, request: engine_pb2.ActionBundle, context: grpc.ServicerContext
    ) -> engine_pb2.ApplyActionsResponse:
        logging.debug(f"ApplyAction: {request}")
        application_results: list[engine_pb2.KeyValue] = []
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
            return
        run = self.runs[run_id]
        for a in request.actions:
            p: str | None = a.WhichOneof("payload")
            if p == "setter":
                additional_parameters: dict[str, str] = {
                    param.name: param.value
                    for param in a.setter.additional_parameters
                }
                try:
                    run.invoke_setter(
                        a.setter.domain,
                        a.setter.setter_name,
                        a.setter.object_id,
                        a.setter.value,
                        additional_parameters,
                    )
                except (AttributeError, TypeError) as e:
                    logging.warning(
                        f"Received a malformed setter request: {a.setter.domain}."
                        f"{a.setter.setter_name}({a.setter.object_id}, "
                        f"{a.setter.value}, {a.setter.additional_parameters})"
                    )
                    # await context.abort(
                    #     grpc.StatusCode.INVALID_ARGUMENT,
                    #     "Setter not found or request malformed.",
                    # )
                    application_results.append(
                        engine_pb2.KeyValue(
                            key="Error",
                            value=Value(string_value=f"Setter: {str(e)}"),
                        )
                    )

                application_results.append(
                    engine_pb2.KeyValue(
                        key="Info",
                        value=Value(
                            string_value=f"Setter Called {a.setter.domain}.{a.setter.setter_name}"
                            f"({a.setter.object_id}, {a.setter.value}, "
                            f"{a.setter.additional_parameters})"
                        ),
                    )
                )

        frame = self._build_frame(run_id=run_id, metrics=application_results)
        await self.telemetry_queues[run_id].put(frame)
        return engine_pb2.ApplyActionsResponse(run_id=run_id)

    async def StreamTelemetry(
        self, request: engine_pb2.StreamRequest, context: grpc.ServicerContext
    ) -> AsyncIterator[engine_pb2.TelemetryFrame]:
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
        self, request: engine_pb2.StreamRequest, context: grpc.ServicerContext
    ) -> AsyncIterator[engine_pb2.TelemetryFrame]:
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
        request: engine_pb2.SubscribeRequest,
        context: grpc.ServicerContext,
    ) -> engine_pb2.SubscribeResponse:
        if self.subscription_manager is None:
            context.abort(
                grpc.StatusCode.ABORTED, "Subscription Manager not initialised."
            )
            return
        logging.debug(f"Subscribe: {request}")
        additional_parameters = {
            p.name: p.value for p in request.additional_parameters
        }

        if request.name in map(
            lambda x: x.name, self.subscription_manager.subscriptions.values()
        ):
            logging.warning(
                f"Duplicate subscription name "
                f"{request.name}. Rejected request."
            )
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                f"Subscription with name {request.name} already exists.",
            )

        fingerprint = self.subscription_manager.subscribe(
            request.domain,
            request.getter_name,
            request.object_id,
            additional_parameters,
            request.name,
        )

        return engine_pb2.SubscribeResponse(
            subscription_name_or_fingerprint=fingerprint
        )

    async def Unsubscribe(
        self,
        request: engine_pb2.UnsubscribeRequest,
        context: grpc.ServicerContext,
    ) -> engine_pb2.UnsubscribeResponse:
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, "unsubscription not implemented"
        )

    async def RegisterInterrupt(
        self,
        request: engine_pb2.RegisterInterruptRequest,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[engine_pb2.InterruptEvent]:
        """CAUTION: Comparaison value must use the same subfield as the
        collected value, or the interrupt will never trigger
        TODO Improve this!"""
        run_id = request.run_id
        if run_id not in self.runs:
            yield context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"run ${run_id} not found."
            )
        run = self.runs[run_id]

        interrupt_requests: asyncio.Queue[InterruptEvent | None] = (
            asyncio.Queue()
        )

        value = request.trigger_metric.value.string_value
        if value == "":
            value = str(request.trigger_metric.value.number_value)

        new_interrupt = Interrupt(
            request.trigger_metric.name,
            value,
            interrupt_requests,
        )

        run.interrupts[new_interrupt.interrupt_id] = new_interrupt

        while True:
            frame = await interrupt_requests.get()
            if frame is None:
                return
            new_interrupt.active_interrupt_event = InterruptEvent(
                frame.observed_value
            )
            yield engine_pb2.InterruptEvent(
                interrupt_id=new_interrupt.interrupt_id,
                event_id=frame.event_id,
                observed_value=frame.observed_value,
            )
            logging.debug("Issued Interrupt Event")

    async def AcknowledgeInterrupt(
        self,
        request: engine_pb2.AcknowledgeInterruptRequest,
        context: grpc.ServicerContext,
    ) -> engine_pb2.ApplyActionsResponse:
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
                acknowledged_interrupt.active_interrupt_event = None

                new_name = request.new_interrupt_conditions.name
                new_value = request.new_interrupt_conditions.value.string_value
                # new_value = request.new_interrupt_conditions.value
                if new_name != '':
                    acknowledged_interrupt.trigger_metric_name = new_name
                    logging.debug(
                        f"Updated interrupt {acknowledged_interrupt.interrupt_id} "
                        f"trigger name to {new_name}."
                    )
                if new_value != '':
                    acknowledged_interrupt.trigger_metric_value = new_value
                    logging.debug(
                        f"Updated interrupt {acknowledged_interrupt.interrupt_id} "
                        f"trigger value to {new_value}."
                    )

                acknowledged_interrupt.interrupt_requests.task_done()

                return await self.ApplyActions(request.actions, context)
                # logging.info(f"Interrupt action would occur here: {request.actions}")
                # return engine_pb2.ApplyActionsResponse()
            except Exception as e:
                await context.abort(
                    grpc.StatusCode.ABORTED, "Error executing interrupt."
                )

    async def CancelInterrupt(
        self,
        request: engine_pb2.CancelInterruptRequest,
        context: grpc.ServicerContext,
    ) -> engine_pb2.CancelInterruptResponse:

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
            return engine_pb2.CancelInterruptResponse(
                interrupt_id=removed_interrupt.interrupt_id
            )

        await context.abort(
            grpc.StatusCode.NOT_FOUND,
            f"Interrupt with id {request.interrupt_id} not found.",
        )

    async def _run_loop(self, run_id: str, max_steps: int) -> None:
        run = self.runs[run_id]
        q = self.telemetry_queues[run_id]

        if self.subscription_manager is None:
            raise RuntimeError("Subscription manager not initialised.")

        try:
            # run.start(max_steps=max_steps) # run already started in create run
            for _ in range(max_steps):
                step, sim_time_s, metrics = run.tick()

                try:
                    failed_getters_and_exceptions = (
                        await self.subscription_manager.collect()
                    )
                    await self.subscription_manager.queue_recent_collect()

                    # decision for now to run interrupts
                    # after collecting subscriptions.
                    for i in run.interrupts.values():
                        recent_collection = (
                            self.subscription_manager.lookup_recent_collection(
                                i.trigger_metric_name
                            )
                        )
                        # recent_collection = Value(
                        #     string_value=(
                        #         self.subscription_manager.lookup_recent_collection(
                        #             i.trigger_metric_name
                        #         )
                        #     )
                        # )
                        assert isinstance(recent_collection, str)
                        assert isinstance(i.trigger_metric_value, str)

                        if recent_collection == i.trigger_metric_value:
                            event = InterruptEvent(
                                observed_value=Value(
                                    string_value=recent_collection
                                )
                            )
                            await i.interrupt_requests.put(event)

                            # need to wait for interrupts to be processed before continuing
                            try:
                                await asyncio.wait_for(
                                    i.interrupt_requests.join(), 1500
                                )
                            except TimeoutError:
                                logging.warning(
                                    f"Interrupt execution timed out!"
                                )

                except Exception as e:
                    logging.warning(
                        f"Failed to collect subscribed metrics: {str(e)}"
                    )

                    # for the moment, crash when problem collecting subscriptions
                    # raise e

                if len(failed_getters_and_exceptions) > 0:
                    errors = []
                    for sub, exception in failed_getters_and_exceptions:
                        errors.append(
                            engine_pb2.KeyValue(
                                key=f"Error",
                                value=Value(
                                    string_value=f"Failed to collect for {sub.name}: "
                                    f"{exception}: {exception.__cause__}"
                                ),
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
                        engine_pb2.KeyValue(
                            key=k, value=Value(number_value=float(v))
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
    engine_pb2_grpc.add_EngineServiceServicer_to_server(EngineService(), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    await server.wait_for_termination()


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    asyncio.run(serve())


if __name__ == "__main__":
    main()
