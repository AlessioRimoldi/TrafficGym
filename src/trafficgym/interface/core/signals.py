from django.db.models.signals import post_save, m2m_changed
from django.db import transaction
from django.dispatch import receiver
from .models import RunRequest, Scenario, RunStatus
from typing import Any
import logging

logger = logging.getLogger(__name__)


@receiver(post_save, sender=RunRequest)
def enqueue_run_request(instance: RunRequest, created: bool, **_: Any) -> None:
    from trafficgym.interface.core.tasks import process_run_request

    logger.info(f"Signal triggered with {instance}")

    if not created:
        return

    if instance.status != RunStatus.PENDING:
        return

    transaction.on_commit(lambda: process_run_request.delay(str(instance.id)))
