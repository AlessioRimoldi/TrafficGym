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


class LogPersistenceHandler(logging.Handler):
    engine_run_id: str | None

    def __init__(self, run: RunRequest) -> None:
        super().__init__()
        self.run = run
        self.engine_run_id = None
        self._log_queue: queue.Queue[logging.LogRecord] = queue.Queue()

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

    def set_engine_run_id(self, engine_run_id: str) -> None:
        self.engine_run_id = engine_run_id

    def flush_queue(self) -> None:
        while not self._log_queue.empty():
            record = self._log_queue.get_nowait()
            try:
                self.emit(record)
            except Exception as e:
                self._internal_logger.error(f"Failed to emit log: {e}")

    def deregister(self) -> None:
        # self._listener.stop()

        for logger in self.loggers.values():
            logger.removeHandler(self._queue_handler)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            event_time = datetime.fromtimestamp(record.created, tz=timezone.utc)

            if record.name not in self.logger_names:
                WorkerLogEntry.objects.create(
                    run_request=self.run,
                    event_time=event_time,
                    level=record.levelname,
                    message=self.format(record),
                )
                return

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
                RPCLogEntry.objects.create(
                    run_request=self.run,
                    engine_run_id=self.engine_run_id,
                    event_time=event_time,
                    direction=getattr(record, "direction"),
                    rpc_name=getattr(record, "rpc_name"),
                    rpc_call_id=getattr(record, "rpc_call_id"),
                    payload=getattr(record, "payload"),
                )
            elif record.name == "subscription":
                SubscriptionLogEntry.objects.create(
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
            elif record.name == "telemetry":
                TelemetryLogEntry.objects.create(
                    run_request=self.run,
                    engine_run_id=self.engine_run_id,
                    event_time=event_time,
                    simulation_time=getattr(record, "simulation_time"),
                    simulation_step=getattr(record, "simulation_step"),
                    telemetry_name=getattr(record, "telemetry_name"),
                    payload=getattr(record, "payload"),
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
