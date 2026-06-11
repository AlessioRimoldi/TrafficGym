import tempfile
import uuid
import shutil
import importlib.util
import hashlib
import logging

from celery import shared_task, Task
from celery.signals import task_failure
from django.db import transaction
from django.utils import timezone
from django.core.files import File
from pathlib import Path
from django.core.files.storage import default_storage
from typing import Any, ParamSpec, cast
from billiard.exceptions import WorkerLostError

from trafficgym.engine.adapters.libsumo_adapter import LibsumoAdapter
from trafficgym.engine.ports.simulation import RunConfig
from trafficgym.engine.transformations.registry import REGISTRY, resolve_inputs, Runtime
from trafficgym.engine.experiment import Experiment
from trafficgym.interface.core.models import (
    RunRequest,
    RunExecution,
    Artefact,
    Scenario,
    TransformationRequest,
    TransformationOutput,
    TERMINAL_STATES,
)
from trafficgym.interface.core.logging_setup import (
    BaseLogPersistenceHandler,
    LogPersistenceHandlerRunRequest,
    LogPersistenceHandlerRunExecution,
    LogPersistenceHandlerTransformRequest,
)

def _compute_file_sha256(file: File) -> str:
    hasher = hashlib.sha256()

    for chunk in file.chunks():
        hasher.update(chunk)

    return hasher.hexdigest()


def materialise_artefacts(
    artefacts: list[Artefact], target_dir: Path
) -> dict[str, Path]:
    sha256_to_paths: dict[str, Path] = {}

    for artefact in artefacts:
        filename = str(artefact.original_name)
        local_path = target_dir / filename

        with default_storage.open(artefact.file.name, "rb") as src:
            sha256 = _compute_file_sha256(src)

            if sha256 != artefact.sha256:
                raise ValueError(
                    f"SHA256 mismatch for Artefact {artefact.original_name}: "
                    f"expected {artefact.sha256}, got {sha256}"
                )

            src.seek(0)

            with open(local_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

        sha256_to_paths[cast(str, artefact.sha256)] = local_path

    return sha256_to_paths


P = ParamSpec("P")


def _maybe_complete_run_request(run_request_id: str | uuid.UUID) -> None:
    with transaction.atomic():
        run_request = RunRequest.objects.select_for_update().get(id=run_request_id)
        if run_request.status in ["COMPLETE", "FAILED"]:
            return
        remaining = RunExecution.objects.filter(
            run_request=run_request,
            status__in=["PENDING", "RUNNING"],
        ).count()
        if remaining > 0:
            return
        has_failures = RunExecution.objects.filter(
            run_request=run_request, status="FAILED"
        ).exists()
        run_request.status = "FAILED" if has_failures else "COMPLETE"
        run_request.finished_at = timezone.now()
        run_request.save(update_fields=["status", "finished_at"])

@task_failure.connect
def on_task_failure(
    task_id: str,
    exception: BaseException,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    sender: Task[Any, Any] | None = None,
    **kw: Any,
) -> None:
    if not isinstance(exception, WorkerLostError):
        return
    if getattr(sender, "name", "") != "trafficgym.interface.core.tasks.process_run_execution":
        return
    if len(args) < 2:
        return

    run_request_id: str | uuid.UUID = args[0]
    execution_id: str | uuid.UUID = args[1]

    RunExecution.objects.filter(
        id=execution_id, status="RUNNING"
    ).update(status="FAILED", finished_at=timezone.now())

    _maybe_complete_run_request(run_request_id)


class RunRequestTask(Task[P, None]):
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
                run_request = RunRequest.objects.select_for_update().get(id=run_request_id)
                if run_request.status not in ["COMPLETE", "FAILED"]:
                    run_request.status = "FAILED"
                    run_request.finished_at = timezone.now()
                    run_request.save(update_fields=["status", "finished_at"])
                RunExecution.objects.filter(
                    run_request_id=run_request_id, status="PENDING"
                ).update(status="FAILED", finished_at=timezone.now())
            except RunRequest.DoesNotExist:
                pass


class RunExecutionTask(Task[P, None]):
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
        execution_id = args[1]

        with transaction.atomic():
            RunExecution.objects.filter(
                id=execution_id,
                status__not__in=TERMINAL_STATES,
            ).update(status="FAILED", finished_at=timezone.now())

        _maybe_complete_run_request(run_request_id)


@shared_task(bind=True, base=RunRequestTask)
def process_run_request(
    self: RunRequestTask[tuple[str | uuid.UUID], None], run_request_id: str | uuid.UUID
) -> None:
    handler: BaseLogPersistenceHandler | None = None
    try:
        with transaction.atomic():
            run_request = (
                RunRequest.objects.select_for_update()
                .get(id=run_request_id)
            )

            handler = LogPersistenceHandlerRunRequest(run_request)

            if run_request.status != "PENDING":
                return

            run_request.status = "RUNNING"
            run_request.worker_id = uuid.uuid4()
            run_request.started_at = timezone.now()
            run_request.save(update_fields=["status", "worker_id", "started_at"])

        execution_ids = list(
            run_request.executions.filter(status="PENDING").values_list("id", flat=True)
        )

        for exec_id in execution_ids:
            process_run_execution.delay(str(run_request_id), str(exec_id))

    except Exception as e:
        logging.error(f"{e}", exc_info=True)
        raise

    finally:
        if handler is not None:
            handler.flush_queue()
            handler.deregister()


@shared_task(bind=True, base=RunExecutionTask)
def process_run_execution(
    self: RunExecutionTask[tuple[str | uuid.UUID, str | uuid.UUID], None],
    run_request_id: str | uuid.UUID,
    execution_id: str | uuid.UUID,
) -> None:
    handler: BaseLogPersistenceHandler | None = None
    adapter: LibsumoAdapter | None = None
    try:
        engine_run_id = uuid.uuid4()

        with transaction.atomic():
            execution = RunExecution.objects.select_for_update().get(id=execution_id)
            if execution.status != "PENDING":
                return
            execution.status = "RUNNING"
            execution.engine_run_id = engine_run_id
            execution.started_at = timezone.now()
            execution.save(update_fields=["status", "engine_run_id", "started_at"])

        handler = LogPersistenceHandlerRunExecution(execution, engine_run_id)

        run_request = (
            RunRequest.objects
            .select_related("scenario")
            .prefetch_related("scenario__artefacts")
            .select_related("experiment")
            .prefetch_related("experiment__artefact")
            .get(id=run_request_id)
        )

        scenario = run_request.scenario
        artefacts_from_scenario = scenario.artefacts.all()
        experiment_artefact = run_request.experiment.artefact
        all_artefacts = [*artefacts_from_scenario, experiment_artefact]

        sumocfg_list = list(
            artefacts_from_scenario.filter(original_name__iendswith=".sumocfg")
        )
        if len(sumocfg_list) != 1:
            raise ValueError("Scenario must contain exactly one .sumocfg!")

        with tempfile.TemporaryDirectory() as run_dir:
            run_dir_path = Path(run_dir)
            sha256_to_paths = materialise_artefacts(all_artefacts, run_dir_path)
            sumocfg_path = sha256_to_paths[cast(str, sumocfg_list[0].sha256)]
            experiment_path = sha256_to_paths[cast(str, experiment_artefact.sha256)]

            spec = importlib.util.spec_from_file_location("experiment_module", experiment_path)
            if spec is None:
                raise FileNotFoundError("Could not find experiment file")
            if spec.loader is None:
                raise ValueError("Could not find experiment loader")

            experiment_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(experiment_module)

            ExperimentClass = getattr(experiment_module, str(run_request.experiment.name), None)
            if ExperimentClass is None:
                raise ValueError(
                    f"Experiment Artefact does not define {run_request.experiment.name}"
                )
            if not issubclass(ExperimentClass, Experiment):
                raise TypeError(
                    f"{run_request.experiment.name} must be a subclass of Experiment"
                )

            sumo_binary = "sumo-gui" if run_request.open_gui else "sumo"
            cfg = RunConfig(sumocfg_path=str(sumocfg_path), sumo_binary=sumo_binary, seed=cast(int, execution.seed))
            adapter = LibsumoAdapter(cfg, step_length_ms=cast(int, run_request.step_length_ms))
            experiment_instance = ExperimentClass()
            sub_logger = logging.getLogger("subscription")
            execution_pk = execution.pk

            def log_step(step: int, sim_time: float, _: Any) -> None:
                for name, value in experiment_instance.poll(adapter).items():
                    sub_logger.info(
                        name,
                        extra={
                            "simulation_step": step,
                            "simulation_time": sim_time,
                            "subscription_fingerprint": name,
                            "payload": value,
                        }
                    )
                if step % 10 == 0:
                    RunExecution.objects.filter(pk=execution_pk).update(current_step=step)

            adapter._after_tick = log_step
            adapter.start()
            experiment_instance.run(adapter=adapter)

        with transaction.atomic():
            execution.status = "COMPLETE"
            execution.finished_at = timezone.now()
            execution.save(update_fields=["status", "finished_at"])

        _maybe_complete_run_request(run_request_id)

    except Exception as e:
        logging.error(f"{e}", exc_info=True)
        raise

    finally:
        # Mark FAILED before closing the adapter — libsumo can SIGABRT on close
        # after an error, which would kill the process before the DB update runs.
        try:
            updated = RunExecution.objects.filter(
                id=execution_id, status="RUNNING"
            ).update(status="FAILED", finished_at=timezone.now())
            if updated:
                _maybe_complete_run_request(run_request_id)
        except Exception:
            pass
        if adapter is not None:
            try:
                adapter.close()
            except Exception:
                pass
        if handler is not None:
            handler.flush_queue()
            handler.deregister()


@shared_task
def derive_from_artefact(artefact_transformation_request_id: str, scenario_id: str | None = None) -> None:
    handler: LogPersistenceHandlerTransformRequest | None = None
    try:
        import asyncio

        with transaction.atomic():
            transformation_request = (
                TransformationRequest.objects.select_for_update()
                .prefetch_related("input_artefacts")
                .get(id=artefact_transformation_request_id)
            )

            handler = LogPersistenceHandlerTransformRequest(
                transformation_request
            )

            if transformation_request.status != "PENDING":
                return

            transformation_request.status = "PREPARING"
            transformation_request.worker_id = uuid.uuid4()
            transformation_request.started_at = timezone.now()

            transformation_request.save(
                update_fields=["status", "worker_id", "started_at"]
            )

        bindings = transformation_request.input_bindings.select_related(
            "artefact"
        )

        input_map: dict[str, Artefact] = {
            str(b.input_name): b.artefact for b in bindings
        }

        artefacts = list({a.sha256: a for a in input_map.values()}.values())

        with tempfile.TemporaryDirectory() as run_dir:
            run_dir_path = Path(run_dir)

            sha256_to_paths = materialise_artefacts(artefacts, run_dir_path)

            input_name_to_path = {
                name: str(sha256_to_paths[str(artefact.sha256)])
                for name, artefact in input_map.items()
            }

            with transaction.atomic():
                transformation_request.status = "RUNNING"
                transformation_request.save(update_fields=["status"])

                spec = REGISTRY[str(transformation_request.method)]
                runtime = Runtime(base_path=run_dir_path)
                resolved = resolve_inputs(spec, transformation_request.parameters, input_name_to_path, runtime)

                derived: dict[str, str] = asyncio.run(
                    spec.handler(resolved, runtime)
                )

                created: list[Artefact] = []
                reused: list[Artefact] = []

                for role, path_str in derived.items():
                    path = Path(path_str)

                    with open(path, "rb") as f:
                        content = f.read()
                        f.seek(0)

                        sha256 = hashlib.sha256(content).hexdigest()

                        artefact = Artefact.objects.filter(
                            sha256=sha256
                        ).first()

                        if artefact is None:
                            safe_name = Path(getattr(f, "name", "file")).name

                            artefact = Artefact.objects.create( # type: ignore
                                file=File(f, name=safe_name)
                            )

                            created.append(artefact)

                        else:
                            reused.append(artefact)

                        TransformationOutput.objects.create(
                            transformation_request=transformation_request,
                            artefact=artefact,
                            role=role,
                        )

                if scenario_id is not None:
                    scenario = Scenario.objects.get(id=scenario_id)
                    output_artefacts = [a for a in created] + [a for a in reused]
                    scenario.artefacts.add(*output_artefacts)

                with transaction.atomic():
                    transformation_request.status = "COMPLETE"
                    transformation_request.finished_at = timezone.now()
                    transformation_request.save(
                        update_fields=["status", "finished_at"]
                    )

    except Exception as e:
        logging.error(str(e), exc_info=True)

        with transaction.atomic():
            transformation_request.status = "FAILED"
            transformation_request.finished_at = timezone.now()
            transformation_request.save(update_fields=["status", "finished_at"])
        raise

    finally:
        if transformation_request.status not in TERMINAL_STATES:
            with transaction.atomic():
                transformation_request.status = "FAILED"
                transformation_request.finished_at = timezone.now()
                transformation_request.save(update_fields=["status", "finished_at"])

        if handler is not None:
            handler.flush_queue()
            handler.deregister()
