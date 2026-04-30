import logging.handlers
from .models import (
    RunRequest,
    RunExecution,
    WorkerLogEntryRunExecution,
    WorkerLogEntryRunRequest,
    WorkerLogEntryTransformRequest,
    RPCLogEntry,
    SubscriptionLogEntry,
    TelemetryLogEntry,
    TransformationRequest,
)
from datetime import datetime, timezone

import queue
import logging
import threading
import traceback
import copy

class PreserveExcInfoQueueHandler(logging.handlers.QueueHandler):
    def prepare(
        self,
        record: logging.LogRecord,
    ) -> logging.LogRecord:
        return copy.copy(record)

class BaseLogPersistenceHandler(logging.Handler):
    engine_run_id: str | None
    BATCH_SIZE = 50

    def __init__(self) -> None:
        super().__init__()
        self._log_queue: queue.Queue[logging.LogRecord] = queue.Queue()
        self._stop_event = threading.Event()
        self._consumer_thread = threading.Thread(
            target=self._consume_queue, daemon=True
        )
        self._consumer_thread.start()

        self._internal_logger = logging.getLogger("log_persistence_internal")
        self._internal_logger.propagate = False  # avoid recursion
        self._internal_logger.addHandler(
            logging.StreamHandler()
        )  # logs to stderr

        self.logger_names: list[str] = ["rpc", "subscription", "telemetry"]

        self.loggers = {
            name: logging.getLogger(name) for name in self.logger_names
        }
        self.loggers["root"] = logging.getLogger()

        # self._queue_handler = logging.handlers.QueueHandler(self._log_queue)
        self._queue_handler = PreserveExcInfoQueueHandler(self._log_queue)

        for logger in self.loggers.values():
            logger.addHandler(self._queue_handler)

        self.loggers["root"].addFilter(
            lambda r: r.name != "log_persistence_internal"
        )

    def _emit_batch(self, records: list[logging.LogRecord]) -> None:
        raise NotImplementedError("Not implemented")

    def _consume_queue(self) -> None:
        batch = []
        while not self._stop_event.is_set() or not self._log_queue.empty():
            try:
                record = self._log_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if record:
                batch.append(record)
                self._log_queue.task_done()

            if len(batch) >= self.BATCH_SIZE:
                self._emit_batch(batch)
                batch = []

        if batch:
            self._emit_batch(batch)

    def _extract_exception(
        self,
        record: logging.LogRecord,
    ) -> tuple[str, str]:
        if not record.exc_info:
            return "", ""

        exc_type = record.exc_info[0]

        return (
            exc_type.__name__ if exc_type else "",
            "".join(traceback.format_exception(*record.exc_info)),
        )

    def flush_queue(self) -> None:
        self._log_queue.join()

    def deregister(self) -> None:
        self._stop_event.set()
        self._consumer_thread.join()

        for logger in self.loggers.values():
            logger.removeHandler(self._queue_handler)


class LogPersistenceHandlerTransformRequest(BaseLogPersistenceHandler):
    def __init__(self, transform_request: TransformationRequest):
        super().__init__()
        self.transform_request = transform_request

    def _emit_batch(self, records: list[logging.LogRecord]) -> None:
        worker_objs = []

        for record in records:
            try:
                event_time = datetime.fromtimestamp(
                    record.created, tz=timezone.utc
                )

                exception_type, tb = self._extract_exception(record)

                worker_objs.append(
                    WorkerLogEntryTransformRequest(
                        transform_request=self.transform_request,
                        event_time=event_time,
                        level=record.levelname,
                        message=record.getMessage(),
                        exception_type=exception_type,
                        traceback=tb,
                    )
                )
                continue

            except Exception as e:
                self._internal_logger.error(
                    f"Failed to create log message for {record.name}: {e}"
                )

        if worker_objs:
            WorkerLogEntryTransformRequest.objects.bulk_create(worker_objs)


class LogPersistenceHandlerRunRequest(BaseLogPersistenceHandler):
    def __init__(self, run_request: RunRequest):
        super().__init__()
        self.run_request = run_request

    def _emit_batch(self, records: list[logging.LogRecord]) -> None:
        worker_objs = []

        for record in records:
            try:
                event_time = datetime.fromtimestamp(
                    record.created, tz=timezone.utc
                )

                exception_type, tb = self._extract_exception(record)

                worker_objs.append(
                    WorkerLogEntryRunRequest(
                        run_request=self.run_request,
                        event_time=event_time,
                        level=record.levelname,
                        message=record.getMessage(),
                        exception_type=exception_type,
                        traceback=tb,
                    )
                )
                continue

            except Exception as e:
                self._internal_logger.error(
                    f"Failed to create log message for {record.name}: {e}"
                )

        if worker_objs:
            WorkerLogEntryRunRequest.objects.bulk_create(worker_objs)


class LogPersistenceHandlerRunExecution(BaseLogPersistenceHandler):
    def __init__(self, run_execution: RunExecution):
        super().__init__()
        self.run_execution = run_execution

    def set_engine_run_id(self, engine_run_id: str) -> None:
        self.engine_run_id = engine_run_id

    def _emit_batch(self, records: list[logging.LogRecord]) -> None:
        rpc_objs, sub_objs, tele_objs, worker_objs = [], [], [], []

        for record in records:
            try:
                event_time = datetime.fromtimestamp(
                    record.created, tz=timezone.utc
                )

                exception_type, tb = self._extract_exception(record)

                if record.name not in self.logger_names:
                    worker_objs.append(
                        WorkerLogEntryRunExecution(
                            run_execution=self.run_execution,
                            event_time=event_time,
                            level=record.levelname,
                            message=record.getMessage(),
                            exception_type=exception_type,
                            traceback=tb,
                        )
                    )
                    continue

            except Exception as e:
                self._internal_logger.error(
                    f"Failed to create log message for {record.name}: {e}"
                )
            try:
                if self.engine_run_id is None:
                    raise ValueError(
                        f"engine_run_id not linked to logging infrastructure for {record.name}"
                    )

                if record.name == "rpc":
                    rpc_objs.append(
                        RPCLogEntry(
                            run_execution=self.run_execution,
                            engine_run_id=self.engine_run_id,
                            event_time=event_time,
                            direction=getattr(record, "direction"),
                            rpc_name=getattr(record, "rpc_name"),
                            rpc_call_id=getattr(record, "rpc_call_id"),
                            payload=getattr(record, "payload"),
                        )
                    )
                elif record.name == "subscription":
                    sub_objs.append(
                        SubscriptionLogEntry(
                            run_execution=self.run_execution,
                            engine_run_id=self.engine_run_id,
                            event_time=event_time,
                            simulation_time=getattr(record, "simulation_time"),
                            simulation_step=getattr(record, "simulation_step"),
                            subscription_fingerprint=getattr(
                                record, "subscription_fingerprint"
                            ),
                            payload=getattr(record, "payload"),
                        )
                    )
                elif record.name == "telemetry":
                    tele_objs.append(
                        TelemetryLogEntry(
                            run_execution=self.run_execution,
                            engine_run_id=self.engine_run_id,
                            event_time=event_time,
                            simulation_time=getattr(record, "simulation_time"),
                            simulation_step=getattr(record, "simulation_step"),
                            telemetry_name=getattr(record, "telemetry_name"),
                            payload=getattr(record, "payload"),
                        )
                    )
                else:
                    self._internal_logger.error(
                        f"Unknown log type invoked ({record.name}): {self.format(record)}"
                    )

            except AttributeError as e:
                self._internal_logger.error(
                    f"Failed to create log message for {record.name}: parameters missing! \n {e}"
                )
            except Exception as e:
                self._internal_logger.error(
                    f"Failed to create log message for {record.name}: {e}"
                )

        if rpc_objs:
            RPCLogEntry.objects.bulk_create(rpc_objs)

        if sub_objs:
            SubscriptionLogEntry.objects.bulk_create(sub_objs)

        if tele_objs:
            TelemetryLogEntry.objects.bulk_create(tele_objs)

        if worker_objs:
            WorkerLogEntryRunExecution.objects.bulk_create(worker_objs)
