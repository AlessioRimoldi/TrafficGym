import tempfile
import uuid
import shutil
import importlib.util
import grpc.aio
import hashlib
import logging
import random

from celery import shared_task, Task
from django.db import transaction
from django.utils import timezone
from django.core.files import File
from django.conf import settings
from pathlib import Path
from django.core.files.storage import default_storage
from typing import Type, Any, ParamSpec, cast

from trafficgym.api import engine_pb2_grpc
from trafficgym.experiment_sdk.experiments.base import Experiment
from trafficgym.engine.client.driver import EngineDriver
from trafficgym.interface.core.models import RunRequest, RunExecution, Scenario
from trafficgym.interface.core.logging_setup import (
    BaseLogPersistenceHandler,
    LogPersistenceHandlerRunRequest,
    LogPersistenceHandlerRunExecution,
)

# event_logger = logging.getLogger("event")
# subscription_logger = logging.getLogger("subscription")
# telemetry_logger = logging.getLogger("telemetry")


async def _async_process(
    execution: RunExecution,
    log_handler: LogPersistenceHandlerRunExecution,
    sumocfg_path: Path,
    ExperimentClass: Type[Experiment],
) -> str:
    async with grpc.aio.insecure_channel("127.0.0.1:50051") as channel:
        stub = engine_pb2_grpc.EngineServiceStub(channel)

        engine_driver = EngineDriver(stub)

        async with engine_driver.create_run(
            str(sumocfg_path), "sumo", 1000, cast(int, execution.seed)
        ) as run:
            log_handler.set_engine_run_id(run.run_id)
            await ExperimentClass(run).run_experiment()
            return run.run_id


def _compute_file_sha256(file: File) -> str:
    hasher = hashlib.sha256()

    for chunk in file.chunks():
        hasher.update(chunk)

    return hasher.hexdigest()


P = ParamSpec("P")


class RunTask(Task[P, None]):
    abstract = True

    def on_failure(
        self,
        exc: BaseException,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: Any,
    ) -> None:
        run_request_id = args[0]

        with transaction.atomic():
            try:
                run = RunRequest.objects.get(id=run_request_id)

                if run.status not in ["COMPLETE", "FAILED"]:
                    run.status = "FAILED"
                    run.finished_at = timezone.now()
                    run.save(update_fields=["status", "finished_at"])

            except RunRequest.DoesNotExist:
                pass


@shared_task(bind=True, base=RunTask)
def process_run_request(
    self: RunTask[tuple[str | uuid.UUID], None], run_request_id: str | uuid.UUID
) -> None:
    handler: BaseLogPersistenceHandler | None = None
    try:
        import asyncio

        run_request = (
            RunRequest.objects.select_for_update()
            .select_related("scenario")
            .prefetch_related("scenario__artefacts")
            .select_related("experiment")
            .prefetch_related("experiment__artefact")
            .get(id=run_request_id)
        )

        handler = LogPersistenceHandlerRunRequest(run_request)

        if run_request.status != "PENDING":
            return

        with transaction.atomic():
            run_request.status = "PREPARING"
            run_request.worker_id = uuid.uuid4()
            run_request.started_at = timezone.now()

            run_request.save(
                update_fields=["status", "worker_id", "started_at"]
            )

        scenario = run_request.scenario

        artefacts_from_scenario = scenario.artefacts.all()

        experiment_artefact = run_request.experiment.artefact

        all_artefacts = [*artefacts_from_scenario, experiment_artefact]

        sumocfg_artefacts = artefacts_from_scenario.filter(
            original_name__iendswith=".sumocfg"
        )

        sumocfg_list = list(sumocfg_artefacts)

        if len(sumocfg_list) != 1:
            raise ValueError("Scenario must contain exactly one .sumocfg!")

        sumocfg_file = sumocfg_list[0]

        with tempfile.TemporaryDirectory() as run_dir:

            run_dir_path = Path(run_dir)

            artefact_paths = {}

            for artefact in all_artefacts:
                filename = str(artefact.original_name)

                local_path = run_dir_path / filename

                with default_storage.open(artefact.file.name, "rb") as src:
                    sha256 = _compute_file_sha256(src)

                    if sha256 != artefact.sha256:
                        raise ValueError(
                            f"SHA256 mismatch for Artefact {artefact.original_name} "
                            f"({local_path}): expected {artefact.sha256}, got {sha256}"
                        )

                    src.seek(0)
                    with open(local_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)

                artefact_paths[artefact.sha256] = str(local_path)

            sumocfg_file_name = str(sumocfg_file.original_name)
            experiment_file_name = str(experiment_artefact.original_name)

            sumocfg_path = run_dir_path / sumocfg_file_name
            experiment_path = run_dir_path / experiment_file_name

            logging.debug(sumocfg_path)
            logging.debug(experiment_path)
            logging.debug(artefact_paths)

            spec = importlib.util.spec_from_file_location(
                "experiment_module", experiment_path
            )

            if spec is None:
                raise FileNotFoundError("Could not find experiment file")

            if spec.loader is None:
                raise ValueError("Could not find experiment loader")

            experiment_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(experiment_module)

            ExperimentClass = getattr(
                experiment_module, str(run_request.experiment.name), None
            )

            if ExperimentClass is None:
                raise ValueError(
                    f"Experiment Artefact does not define {run_request.experiment.name}"
                )

            if not issubclass(ExperimentClass, Experiment):
                raise TypeError(
                    f"{run_request.experiment.name} must be a subclass of Experiment"
                )

            with transaction.atomic():
                run_request.status = "RUNNING"
                run_request.save(update_fields=["status"])

            for i in range(cast(int, run_request.rerun_count)):
                seed = int(random.random() * (2**31 - 1))
                RunExecution.objects.create(
                    run_request=run_request, seed=seed, status="PENDING"
                )

            for execution in run_request.executions.all():
                with transaction.atomic():
                    execution.status = "RUNNING"
                    execution.save(update_fields=["status"])

                handler.flush_queue()
                handler.deregister()
                handler = LogPersistenceHandlerRunExecution(execution)

                try:
                    engine_run_id = asyncio.run(
                        _async_process(
                            execution,
                            handler,
                            sumocfg_path,
                            ExperimentClass,
                        )
                    )

                    with transaction.atomic():
                        execution.status = "COMPLETE"
                        execution.finished_at = timezone.now()
                        execution.engine_run_id = engine_run_id
                        execution.save(
                            update_fields=[
                                "status",
                                "finished_at",
                                "engine_run_id",
                            ]
                        )

                except Exception as e:
                    logging.error(f"{e}", exc_info=True)
                    with transaction.atomic():
                        execution.status = "FAILED"
                        execution.finished_at = timezone.now()
                        execution.save(update_fields=["status", "finished_at"])
                    raise Exception(
                        f"Run execution {i + 1} of {run_request.rerun_count} failed. ({execution.id})"
                    ) from e
                finally:
                    execution.save()
                    handler.flush_queue()
                    handler.deregister()
                    handler = LogPersistenceHandlerRunRequest(run_request)

            with transaction.atomic():
                run_request.status = "COMPLETE"
                run_request.finished_at = timezone.now()
                run_request.save(update_fields=["status", "finished_at"])

    except Exception as e:
        logging.error(f"{e}", exc_info=True)
        with transaction.atomic():
            run_request.status = "FAILED"
            run_request.finished_at = timezone.now()
            run_request.save(update_fields=["status", "finished_at"])
        raise

    finally:
        if handler is not None:
            handler.flush_queue()
            handler.deregister()


@shared_task
def generate_scenario_plot(scenario_id: str) -> None:
    import SumoNetVis  # type: ignore[import-untyped]
    import matplotlib.pyplot as plt

    scenario = Scenario.objects.get(id=scenario_id)

    net_files = scenario.artefacts.filter(
        original_name__iendswith=".net.xml"
    ).order_by("original_name")

    if net_files.count() > 1:
        logging.warning(
            'Multiple ".net.xml" files detected in scenario. Preview image will be for first one.'
        )

    net_file = net_files.first()

    if net_file is None:
        logging.error(
            'No ".net.xml" file found in scenario. Preview image unavailable'
        )
        return

    with tempfile.TemporaryDirectory() as run_dir:

        run_dir_path = Path(run_dir)

        filename = str(net_file.original_name)
        local_path = run_dir_path / filename

        with default_storage.open(net_file.file.name, "rb") as src:
            sha256 = _compute_file_sha256(src)

            if sha256 != net_file.sha256:
                raise ValueError(
                    f"SHA256 mismatch for Artefact {net_file.original_name} "
                    f"({local_path}): expected {net_file.sha256}, got {sha256}"
                )

            src.seek(0)
            with open(local_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

        net_path = str(local_path)

        _, ax = plt.subplots()

        net = SumoNetVis.Net(net_path)
        net.plot(ax=ax)

        output_dir = Path(settings.MEDIA_ROOT) / "scenario_plots"
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"scenario_{scenario.id}.png"
        filepath = output_dir / filename

        ax.set_aspect("equal")
        ax.axis("off")
        plt.savefig(filepath, dpi=500, bbox_inches="tight")
        plt.close()
        logging.info(f"Saved scenario plot to {filepath}")

        # fig, ax = plt.subplots(figsize=(8,6))
        # net.plot(ax=ax)  # pass axes if supported
        # fig.savefig(filepath, dpi=150, bbox_inches='tight')
        # plt.close(fig)
