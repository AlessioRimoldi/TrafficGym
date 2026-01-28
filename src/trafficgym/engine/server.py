from __future__ import annotations
import asyncio
from typing import Dict
from enum import Enum

import grpc

from .kernel import RunConfig, RunState
from ..api import engine_pb2, engine_pb2_grpc
from libsumo import DOMAINS

import logging


class InvalidGetterError(Exception):
    def __init__(self, message_or_exception: str | Exception):
        if isinstance(message_or_exception, Exception):
            self.original = message_or_exception
            message = str(message_or_exception)
        else:
            self.original = None
            message = message_or_exception

        super().__init__(message)


def raise_async_except(task, context):
    if task.exception() is not None:
        context.abort(grpc.StatusCode.UNKNOWN, "An Exception Occured")


Domain = Enum("Domain", list(map(lambda x: x.__name__, DOMAINS)))


class Subscription:
    def __init__(
        self,
        domain: Domain,
        getter_name: str,
        object_id: str | None = None,
        additional_param: dict | None = None,
        name: str | None = None,
    ):
        self.__name = name
        self.domain = domain
        self.getter_name = getter_name
        self.object_id = object_id
        self.additional_param = additional_param or {}

    @property
    def name(self):
        """Returns the name of the subscription if set, otherwise the fingerprint"""
        return self.__name or self.fingerprint()

    def fingerprint(self):
        return f"{str(self.domain)}.{self.getter_name}_{self.object_id}"

    def collect(self, run_state: RunState):
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
                self.additional_param,
            )
        except AttributeError as e:
            raise InvalidGetterError("Unknown getter") from e
            # in future, we can check the getter and name exist before calling
            # and also send the client the available getters before they exec.

        return collected


class SubscriptionManager:
    subscriptions: dict[str, Subscription]
    metrics: dict[str, list[any]]

    def __init__(
        self, run_state: RunState, subscription_queues: list[asyncio.Queue]
    ):
        self.run_state = run_state
        self.subscription_queues = subscription_queues

        self.subscriptions = {}
        self.metrics = {}

        self.telemetryStep = 0

    async def collect(self):
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

    async def queue_recent_collect(self):
        q = self.subscription_queues[self.run_state.run_id]
        frame = engine_pb2.TelemetryFrame(
            run_id=self.run_state.run_id,
            step=self.run_state.step,
            # sim_time_s=self.run_state.step,  # temporary
            metrics=[
                engine_pb2.KeyValue(key=k, string_value=str(v))
                for k, v in self.newMetrics.items()
            ],
        )
        await q.put(frame)

    def subscribe(
        self,
        domain: Domain,
        getter_name: str,
        object_id: str | None = None,
        additional_params: dict = {},
        name: str | None = None,
    ):
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

    def unsubscribe(self, fingerprint):
        if fingerprint not in self.subscriptions:
            logging.warning(
                f"Received a request to unsubscribe from {fingerprint}, "
                f"but the subscription was not found."
            )
            raise Exception("Unsubscribe failed")

        del self.subscriptions[fingerprint]


class EngineService(engine_pb2_grpc.EngineServiceServicer):
    def __init__(self):
        self.runs: Dict[str, RunState] = {}
        self.telemetry_queues: Dict[str, asyncio.Queue] = {}
        self.subscription_queues: Dict[str, asyncio.Queue] = {}
        self.run_tasks: Dict[str, asyncio.Task] = {}
        self.subscription_manager: SubscriptionManager = {}

    async def CreateRun(self, request, context):
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

        # self.subscription_manager = SubscriptionManager(run, self.subscription_queues)
        self.subscription_manager = SubscriptionManager(
            run, self.subscription_queues
        )

        return engine_pb2.CreateRunResponse(
            run_id=run.run_id, input_artifacts=[]
        )

    async def Run(self, request, context):
        logging.debug(f"Run: {request}")
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            await context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        if run_id not in self.run_tasks:
            task = asyncio.create_task(
                self._run_loop(run_id, int(request.max_steps or 1000))
            )
            self.run_tasks[run_id] = task
            await task
            raise_async_except(task, context)
        return engine_pb2.RunResponse(run_id=run_id)

    async def CloseRun(self, request, context):
        logging.debug(f"CloseRun: {request}")
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            await context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        run = self.runs[run_id]

        if run_id in self.run_tasks:
            logging.warning(
                f"Closing Run {run_id}, despite running task for that run"
            )
        run.close()
        run.started = False
        await self.telemetry_queues[run_id].put(None)
        return engine_pb2.CloseRunResponse(run_id=run_id)

    async def ApplyActions(self, request, context):
        logging.debug(f"ApplyAction: {request}")
        application_results = []
        run_id = request.run_id
        if run_id not in self.runs:
            logging.warning(f"Could not find run {run_id}.")
            await context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        run = self.runs[run_id]
        for a in request.actions:
            p = a.WhichOneof("payload")
            if p == "setter":
                try:
                    run.invoke_setter(
                        a.setter.domain,
                        a.setter.setter_name,
                        a.setter.object_id,
                        a.setter.value,
                        a.setter.additional_parameters or {},
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
                        engine_pb2.KeyValue(key="Error", string_value=f"Setter: {str(e)}")
                    )

                application_results.append(
                    engine_pb2.KeyValue(
                        key="Info",
                        string_value=(
                            f"Setter Called {a.setter.domain}.{a.setter.setter_name}"
                            f"({a.setter.object_id}, {a.setter.value}, "
                            f"{a.setter.additional_parameters})"
                        ),
                    )
                )

        frame = engine_pb2.TelemetryFrame(
            run_id=run_id,
            step=run.step,  # EWWWWWWWWWWWW terrible code, will refactor
            # sim_time_s=run.step,  # ditto
            metrics=application_results
        )
        await self.telemetry_queues[run_id].put(frame)
        return engine_pb2.ApplyActionsResponse(run_id=run_id)

    async def Subscribe(self, request, context):
        logging.debug(f"Subscribe: {request}")
        additional_parameters = {
            p.name: p.value for p in request.additional_parameters
        }
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

    # async def Unsubscribe(self, request, context):
    #     await context.abort(
    #         grpc.StatusCode.UNIMPLEMENTED, "unsubscription not implemented"
    #     )

    async def StreamTelemetry(self, request, context):
        logging.debug(f"StreamTelemetry: {request}")
        run_id = request.run_id
        if run_id not in self.telemetry_queues:
            await context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        q = self.telemetry_queues[run_id]
        while True:
            frame = await q.get()
            if frame is None:
                return
            yield frame

    async def StreamSubscriptions(self, request, context):
        logging.debug(f"StreeamSubscription: {request}")
        run_id = request.run_id
        if run_id not in self.subscription_queues:
            await context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        q = self.subscription_queues[run_id]
        while True:
            frame = await q.get()
            if frame is None:
                return
            yield frame

    async def _run_loop(self, run_id: str, max_steps: int):
        run = self.runs[run_id]
        q = self.telemetry_queues[run_id]
        try:
            run.start(max_steps=max_steps)
            for _ in range(max_steps):
                step, sim_time_s, metrics = run.tick()

                try:
                    failed_getters_and_exceptions = (
                        await self.subscription_manager.collect()
                    )
                    await self.subscription_manager.queue_recent_collect()
                except Exception as e:
                    logging.warning(
                        f"Failed to collect subscribed metrics: {str(e)}"
                    )

                    raise e

                if len(failed_getters_and_exceptions) > 0:
                    errors = []
                    for sub, exception in failed_getters_and_exceptions:
                        errors.append(
                            engine_pb2.KeyValue(
                                key=f"Error",
                                string_value=f"Failed to collect for {sub.name}: {exception}: {exception.__cause__}",
                            )
                        )
                        logging.warning(
                            f"Subscription collection failed for {sub.name}"
                        )

                    frame = engine_pb2.TelemetryFrame(
                        run_id=run_id,
                        step=step,
                        # sim_time_s=sim_time_s,
                        metrics=errors,
                    )
                    await q.put(frame)

                frame = engine_pb2.TelemetryFrame(
                    run_id=run_id,
                    step=step,
                    # sim_time_s=sim_time_s,
                    metrics=[
                        engine_pb2.KeyValue(key=k, double_value=float(v))
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


async def serve(host: str = "127.0.0.1", port: int = 50051):
    server = grpc.aio.server()
    engine_pb2_grpc.add_EngineServiceServicer_to_server(EngineService(), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    await server.wait_for_termination()


def main():
    logging.basicConfig(
        level=logging.INFO
    )

    asyncio.run(serve())


if __name__ == "__main__":
    main()
