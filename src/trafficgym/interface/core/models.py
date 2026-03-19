from django.db import models, IntegrityError
from pathlib import Path
from typing import Any, cast
import json
import uuid
import hashlib

# Create your models here.


class Artefact(models.Model):
    sha256 = models.CharField(
        primary_key=True, max_length=64, unique=True, blank=True, editable=False
    )
    file = models.FileField(upload_to="artefacts/")
    original_name = models.CharField(max_length=255, blank=True, editable=False)

    created_at = models.DateTimeField(auto_now_add=True)
    metadata = models.JSONField(blank=True, default=dict)

    def __str__(self) -> str:
        return Path(self.file.name).name

    def compute_sha256(self) -> str:
        hasher = hashlib.sha256()

        for chunk in self.file.chunks():
            hasher.update(chunk)

        return hasher.hexdigest()

    def save(self, *args: Any, **kwargs: Any) -> None:
        if not self._state.adding:
            raise ValueError("Artefacts are Immutable")

        else:
            if not self.file:
                raise ValueError("Artefact must have a file")

            self.sha256 = self.compute_sha256()
            self.original_name = self.file.name
            self.file.name = f"{self.original_name}_{self.sha256}"

        kwargs["force_insert"] = True
        super().save(*args, **kwargs)


class Scenario(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    artefacts = models.ManyToManyField[Artefact, Any](
        Artefact, related_name="scenarios"
    )

    def compute_sha256(self) -> str:
        """The scenario model does not include a hash due to issues hashing
        the many to many relationship.

        Instead, the run_signature will be the authoritative source for run_request
        persistence. It is derived from the scenario artefact hash at the time of
        request creation"""
        hasher = hashlib.sha256()

        for artefact in self.artefacts.order_by("sha256").all():
            hasher.update(cast(str, artefact.sha256).encode())

        return hasher.hexdigest()

    def save(self, *args: Any, **kwargs: Any) -> None:
        if not self._state.adding:
            raise ValueError("Scenarios are immutable")

        try:
            super().save(*args, **kwargs)
        except IntegrityError:
            raise

    def __str__(self) -> str:
        return f"{self.name} ({self.id})"


class Experiment(models.Model):
    sha256 = models.CharField(
        primary_key=True, max_length=64, unique=True, blank=True, editable=False
    )
    name = models.CharField(max_length=64)
    version = models.IntegerField()
    artefact: models.ForeignKey[Artefact] = models.ForeignKey(
        Artefact, on_delete=models.PROTECT
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["name", "version"], name="unique_name_version"
            )
        ]

    def compute_sha256(self) -> str:
        hasher = hashlib.sha256()

        hasher.update(cast(str, self.artefact.sha256).encode())

        return hasher.hexdigest()

    def save(self, *args: Any, **kwargs: Any) -> None:
        if not self._state.adding:
            raise ValueError("Experiments are immutable")

        else:
            if not self.sha256:
                self.sha256 = self.compute_sha256()

        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.name} v{self.version} ({self.sha256})"


class RunStatus(models.TextChoices):
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class RunRequest(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    engine_run_id = models.UUIDField(
        null=True, blank=True, unique=True, editable=False
    )
    worker_id = models.UUIDField(null=True, blank=True, editable=False)

    scenario: models.ForeignKey[Scenario] = models.ForeignKey(
        Scenario, on_delete=models.deletion.PROTECT
    )
    experiment: models.ForeignKey[Experiment] = models.ForeignKey(
        Experiment, on_delete=models.deletion.PROTECT
    )

    simulation_parameters = models.JSONField[Any](blank=True, default=dict)
    status = models.CharField(
        choices=RunStatus.choices,
        default=RunStatus.PENDING,
        blank=True,
        editable=False,
    )
    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    started_at = models.DateTimeField(null=True, blank=True, editable=False)
    finished_at = models.DateTimeField(null=True, blank=True, editable=False)
    run_signature = models.CharField(
        null=True, blank=True, max_length=64, editable=False
    )

    class Meta:
        ordering = ["-created_at"]

    def compute_sha256(self) -> str:
        hasher = hashlib.sha256()

        for hash_str in [
            self.scenario.compute_sha256(),
            self.experiment.sha256,
        ]:
            hasher.update(cast(str, hash_str).encode())

        hasher.update(
            json.dumps(
                self.simulation_parameters,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )

        return hasher.hexdigest()

    # def save(self, *args: Any, **kwargs: Any) -> None:
    #     if self._state.adding:
    #         if not self.run_signature and self.scenario and self.experiment:
    #             self.run_signature = self.compute_sha256()

    #     else:
    #         raise ValueError("RunRequests are immutable")

    #     super().save(*args, **kwargs)

    def __str__(self) -> str:
        return str(self.id)

    #     return f"{self.created_at}: {self.status}"


class WorkerLogEntry(models.Model):
    run_request: models.ForeignKey[RunRequest] = models.ForeignKey(
        RunRequest, on_delete=models.CASCADE, related_name="worker_logs"
    )
    event_time = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=20)
    message = models.TextField()

    class Meta:
        indexes = [models.Index(fields=["run_request", "created_at"])]


class EventLogType(models.TextChoices):
    REQUEST = "REQUEST"
    RESPONSE = "RESPONSE"
    INFO = "INFO"


class RPCLogEntry(models.Model):
    run_request: models.ForeignKey[RunRequest] = models.ForeignKey(
        RunRequest, on_delete=models.CASCADE, related_name="rpc_logs"
    )
    engine_run_id = models.UUIDField()
    event_time = models.DateTimeField()
    rpc_name = models.CharField(max_length=64)
    rpc_call_id = models.UUIDField()
    direction = models.CharField(choices=EventLogType.choices)
    payload = models.JSONField()


class SubscriptionLogEntry(models.Model):
    run_request: models.ForeignKey[RunRequest] = models.ForeignKey(
        RunRequest, on_delete=models.CASCADE, related_name="subscription_logs"
    )
    engine_run_id = models.UUIDField()
    event_time = models.DateTimeField()
    subscription_fingerprint = models.CharField(max_length=256)
    payload = models.JSONField()

    def __str__(self) -> str:
        return f"{self.subscription_fingerprint}"


class TelemetryLogEntry(models.Model):
    run_request: models.ForeignKey[RunRequest] = models.ForeignKey(
        RunRequest, on_delete=models.CASCADE, related_name="telemetry_logs"
    )
    engine_run_id = models.UUIDField()
    event_time = models.DateTimeField()
    telemetry_name = models.CharField(max_length=256)
    payload = models.JSONField()
