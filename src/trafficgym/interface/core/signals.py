from django.db.models.signals import post_save
from django.db import transaction
from django.dispatch import receiver
from django.utils import timezone
from .models import RunRequest, RunStatus, RunExecution, TransformationRequest, WorkerLogEntryRunRequest
from .utils import safe_delay
from typing import Any
import logging
import traceback

logger = logging.getLogger(__name__)


@receiver(post_save, sender=RunRequest)
def enqueue_run_request(instance: RunRequest, created: bool, **_: Any) -> None:
    from trafficgym.interface.core.tasks import process_run_request

    if not created or instance.status != RunStatus.PENDING:
        return

    def on_error(e: Exception) -> None:
        now = timezone.now()
        RunRequest.objects.filter(id=instance.id, status=RunStatus.PENDING).update(
            status=RunStatus.FAILED, finished_at=now,
        )
        RunExecution.objects.filter(
            run_request=instance, status="PENDING"
        ).update(status="FAILED", finished_at=now)
        WorkerLogEntryRunRequest.objects.create(
            run_request_id=instance.id,
            event_time=now,
            level="ERROR",
            message=str(e),
            exception_type=type(e).__name__,
            traceback=traceback.format_exc(),
        )

    transaction.on_commit(lambda: safe_delay(process_run_request, str(instance.id), on_broker_error=on_error))


@receiver(post_save, sender=TransformationRequest)
def enqueue_transformation_request(
    instance: TransformationRequest, created: bool, **_: Any
) -> None:
    from trafficgym.interface.core.tasks import derive_from_artefact

    if not created:
        return

    def on_error(e: Exception) -> None:
        TransformationRequest.objects.filter(id=instance.id, status="PENDING").update(
            status="FAILED", finished_at=timezone.now(),
        )

    transaction.on_commit(lambda: safe_delay(derive_from_artefact, str(instance.id), on_broker_error=on_error))
