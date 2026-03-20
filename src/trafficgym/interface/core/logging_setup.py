import logging.handlers
from .models import (
    RunRequest,
    WorkerLogEntry,
    RPCLogEntry,
    SubscriptionLogEntry,
    TelemetryLogEntry,
)
from datetime import datetime, timezone

import queue
import logging
import threading


class LogPersistenceHandler(logging.Handler):
    engine_run_id: str | None
    BATCH_SIZE = 50

    def __init__(self, run: RunRequest) -> None:
        super().__init__()
        self.run = run
        self.engine_run_id = None
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

        self._queue_handler = logging.handlers.QueueHandler(self._log_queue)

        for logger in self.loggers.values():
            logger.addHandler(self._queue_handler)

        self.loggers["root"].addFilter(
            lambda r: r.name != "log_persistence_internal"
        )

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

    def set_engine_run_id(self, engine_run_id: str) -> None:
        self.engine_run_id = engine_run_id

    def flush_queue(self) -> None:
        self._log_queue.join()

    def deregister(self) -> None:
        self._stop_event.set()
        self._consumer_thread.join()

        for logger in self.loggers.values():
            logger.removeHandler(self._queue_handler)

    def _emit_batch(self, records: list[logging.LogRecord]) -> None:
        rpc_objs, sub_objs, tele_objs, worker_objs = [], [], [], []

        for record in records:
            try:
                event_time = datetime.fromtimestamp(record.created, tz=timezone.utc)

                if record.name not in self.logger_names:
                    worker_objs.append(
                        WorkerLogEntry(
                            run_request=self.run,
                            event_time=event_time,
                            level=record.levelname,
                            message=self.format(record),
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
                            run_request=self.run,
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
                            run_request=self.run,
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
                            run_request=self.run,
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
            WorkerLogEntry.objects.bulk_create(worker_objs)
