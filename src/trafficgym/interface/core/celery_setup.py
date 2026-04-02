import os
from celery import Celery
from celery.app.task import Task
from typing import Any

Task.__class_getitem__ = classmethod(lambda cls, *args, **kwargs: cls)  # type: ignore[attr-defined]

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE", "trafficgym.interface.config.settings"
)

app = Celery("core")
app.config_from_object("django.conf:settings", namespace="CELERY")

app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self: Task[Any, None]) -> None:
    print(f"Request: {self.request!r}")
