from dataclasses import dataclass
from django.core.files import File
from django.core.files.uploadedfile import UploadedFile
from typing import Any, Callable
import hashlib
import logging
import traceback

from .models import Artefact

logger = logging.getLogger(__name__)


def safe_delay(task: Any, *args: Any, on_broker_error: Callable[[Exception], Any], **kwargs: Any) -> None:
    try:
        task.delay(*args, **kwargs)
    except Exception as e:
        msg = f"Broker unreachable — could not dispatch {getattr(task, 'name', task)}: {e}"
        logger.error(msg)
        on_broker_error(Exception(msg))


@dataclass(frozen=True)
class ArtefactResolution:
    artefact: Artefact
    created: bool


def get_or_create_artefact_from_upload(
    uploaded_file: UploadedFile,
) -> ArtefactResolution:
    content = uploaded_file.read()
    uploaded_file.seek(0)

    sha256 = hashlib.sha256(content).hexdigest()

    existing = Artefact.objects.filter(sha256=sha256).first()

    if existing is not None:
        return ArtefactResolution(
            artefact=existing,
            created=False,
        )

    artefact = Artefact.objects.create(
        file=uploaded_file # type: ignore
    )

    return ArtefactResolution(
        artefact=artefact,
        created=True,
    )