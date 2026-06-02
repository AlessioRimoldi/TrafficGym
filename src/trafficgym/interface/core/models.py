from django.db import models, IntegrityError
from django.conf import settings
from pathlib import Path
from typing import Any, cast, TYPE_CHECKING
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

    type = models.CharField(max_length=64, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    metadata = models.JSONField(blank=True, default=dict)

    derivatives: models.Manager[TransformationInput]
    provenance: models.Manager[TransformationOutput]

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
    name = models.CharField(max_length=255, unique=True)
    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    artefacts = models.ManyToManyField[Artefact, Any](
        Artefact, related_name="scenarios"
    )

    run_requests: models.Manager[RunRequest]

    @property
    def image_artefact(self) -> "Artefact | None":
        request = self.image_transformation_request
        if request is None:
            return None
        output = (
            request.output_bindings.select_related("artefact")
            .order_by("id")
            .first()
        )
        return output.artefact if output else None

    @property
    def image_url(self) -> str:
        artefact = self.image_artefact
        return artefact.file.url if artefact else ""

    @property
    def image_transformation_request(self) -> TransformationRequest | None:
        net_artefact = (
            self.artefacts.filter(
                original_name__iendswith=".net.xml"
            )
            .order_by("original_name")
            .first()
        )

        if net_artefact is None:
            return None

        return (
            TransformationRequest.objects.filter(
                method="netpreview",
                input_bindings__artefact=net_artefact,
            )
            .order_by("-created_at")
            .first()
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
    version = models.IntegerField(default=0)
    artefact: models.ForeignKey[Artefact] = models.ForeignKey(
        Artefact, on_delete=models.PROTECT, related_name="experiments"
    )
    total_steps = models.IntegerField(default=-1)

    run_requests: models.Manager[RunRequest]

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
        if not self.sha256:
            self.sha256 = self.compute_sha256()
        if self.version == 0:
            last = Experiment.objects.filter(name=self.name).aggregate(
                models.Max("version")
            )["version__max"]
            self.version = (last or 0) + 1
        if self.total_steps == -1:
            from trafficgym.engine.adapters.counting_adapter import count_experiment_steps
            self.total_steps = count_experiment_steps(self.artefact)
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
    worker_id = models.UUIDField(null=True, blank=True, editable=False)
    scenario: models.ForeignKey[Scenario] = models.ForeignKey(
        Scenario, on_delete=models.deletion.PROTECT, related_name="run_requests"
    )
    experiment: models.ForeignKey[Experiment] = models.ForeignKey(
        Experiment,
        on_delete=models.deletion.PROTECT,
        related_name="run_requests",
    )
    simulation_parameters = models.JSONField[Any](blank=True, default=dict)
    status = models.CharField(
        choices=RunStatus.choices,
        default=RunStatus.PENDING,
        blank=True,
        editable=False,
    )
    rerun_count = models.IntegerField()
    open_gui = models.BooleanField(default=False)
    step_length_ms = models.IntegerField(default=1000)
    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    started_at = models.DateTimeField(null=True, blank=True, editable=False)
    finished_at = models.DateTimeField(null=True, blank=True, editable=False)
    run_signature = models.CharField(
        null=True, blank=True, max_length=64, editable=False
    )

    worker_logs: models.Manager[WorkerLogEntryRunRequest]
    executions: models.Manager[RunExecution]

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


class RunExecution(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    run_request = models.ForeignKey(
        RunRequest, on_delete=models.CASCADE, related_name="executions"
    )
    engine_run_id = models.UUIDField(
        null=True, blank=True, unique=True, editable=False
    )
    worker_id = models.UUIDField(null=True, blank=True, editable=False)
    seed = models.BigIntegerField()
    current_step = models.IntegerField(default=0)
    status = models.CharField(
        choices=RunStatus.choices,
        default=RunStatus.PENDING,
        blank=True,
        editable=False,
    )
    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    started_at = models.DateTimeField(null=True, blank=True, editable=False)
    finished_at = models.DateTimeField(null=True, blank=True, editable=False)

    subscription_logs: models.Manager[SubscriptionLogEntry]
    worker_logs: models.Manager[WorkerLogEntryRunExecution]


class WorkerLogEntryRunRequest(models.Model):
    run_request: models.ForeignKey[RunRequest | None] = models.ForeignKey(
        RunRequest,
        on_delete=models.CASCADE,
        related_name="worker_logs",
        null=True,
    )
    event_time = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=20)
    message = models.TextField()
    exception_type = models.CharField(max_length=255, blank=True)
    traceback = models.TextField(blank=True)

    class Meta:
        indexes = [models.Index(fields=["run_request", "created_at"])]


class WorkerLogEntryRunExecution(models.Model):
    run_execution: models.ForeignKey[RunExecution | None] = models.ForeignKey(
        RunExecution,
        on_delete=models.CASCADE,
        related_name="worker_logs",
        null=True,
    )
    event_time = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=20)
    message = models.TextField()
    exception_type = models.CharField(max_length=255, blank=True)
    traceback = models.TextField(blank=True)

    class Meta:
        indexes = [models.Index(fields=["run_execution", "created_at"])]


class EventLogType(models.TextChoices):
    REQUEST = "REQUEST"
    RESPONSE = "RESPONSE"
    INFO = "INFO"


class RPCLogEntry(models.Model):
    run_execution: models.ForeignKey[RunExecution] = models.ForeignKey(
        RunExecution, on_delete=models.CASCADE, related_name="rpc_logs"
    )
    engine_run_id = models.UUIDField()
    event_time = models.DateTimeField()
    rpc_name = models.CharField(max_length=64)
    rpc_call_id = models.UUIDField()
    direction = models.CharField(choices=EventLogType.choices)
    payload = models.JSONField()


class SubscriptionLogEntry(models.Model):
    run_execution: models.ForeignKey[RunExecution] = models.ForeignKey(
        RunExecution, on_delete=models.CASCADE, related_name="subscription_logs"
    )
    engine_run_id = models.UUIDField()
    event_time = models.DateTimeField()
    simulation_time = models.FloatField()
    simulation_step = models.IntegerField()
    subscription_fingerprint = models.CharField(max_length=256)
    payload = models.JSONField()

    def __str__(self) -> str:
        return f"{self.subscription_fingerprint}"

    class Meta:
        indexes = [
            models.Index(fields=["run_execution", "subscription_fingerprint"]),
            models.Index(fields=["run_execution", "subscription_fingerprint", "simulation_time"]),
        ]


class TelemetryLogEntry(models.Model):
    run_execution: models.ForeignKey[RunExecution] = models.ForeignKey(
        RunExecution, on_delete=models.CASCADE, related_name="telemetry_logs"
    )
    engine_run_id = models.UUIDField()
    event_time = models.DateTimeField()
    simulation_time = models.FloatField()
    simulation_step = models.IntegerField()
    telemetry_name = models.CharField(max_length=256)
    payload = models.JSONField()


class TransformationOutput(models.Model):
    transformation_request = models.ForeignKey(
        "TransformationRequest", on_delete=models.CASCADE, related_name="output_bindings",
    )
    artefact: models.ForeignKey[Artefact] = models.ForeignKey(
        Artefact, on_delete=models.CASCADE,
        related_name="provenance",
    )
    role = models.CharField(max_length=64)


class TransformationInput(models.Model):
    transformation_request = models.ForeignKey(
        "TransformationRequest",
        on_delete=models.CASCADE,
        related_name="input_bindings",
    )
    artefact: models.ForeignKey[Artefact] = models.ForeignKey(
        Artefact, on_delete=models.CASCADE,
        related_name="derivatives"
    )
    input_name = models.CharField(max_length=255)

    class Meta:
        unique_together = ("transformation_request", "input_name")


class TransformStatus(models.TextChoices):
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class TransformationRequest(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    worker_id = models.UUIDField(null=True, blank=True, editable=False)
    input_artefacts = models.ManyToManyField[Artefact, Any](
        Artefact,
        through=TransformationInput,
        related_name="transformation_requests",
    )
    input_bindings: models.manager.RelatedManager[TransformationInput]
    method = models.CharField(
        max_length=64,
    )
    spec_snapshot = models.JSONField()
    parameters = models.JSONField()

    worker_logs: models.Manager[WorkerLogEntryTransformRequest]

    output_artefacts = models.ManyToManyField[Artefact, Any](
        Artefact,
        related_name="produced_by",
        blank=True,
        through=TransformationOutput,
    )

    output_bindings: models.manager.RelatedManager[TransformationOutput]

    status = models.CharField(
        choices=TransformStatus.choices,
        default=TransformStatus.PENDING,
        blank=True,
        editable=False,
    )
    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    started_at = models.DateTimeField(null=True, blank=True, editable=False)
    finished_at = models.DateTimeField(null=True, blank=True, editable=False)


class ExperimentGraph(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    version = models.IntegerField(default=0)
    scenario: models.ForeignKey[Scenario] = models.ForeignKey(
        Scenario, on_delete=models.PROTECT, related_name="experiment_graphs"
    )
    experiment: models.ForeignKey[Experiment | None] = models.ForeignKey(
        Experiment,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="experiment_graphs",
    )
    graph = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

    def save(self, *args: Any, **kwargs: Any) -> None:
        if self._state.adding and self.version == 0:
            last = ExperimentGraph.objects.filter(name=self.name).aggregate(
                models.Max("version")
            )["version__max"]
            self.version = (last or 0) + 1
        super().save(*args, **kwargs)


class WorkerLogEntryTransformRequest(models.Model):
    transform_request: models.ForeignKey[TransformationRequest | None] = (
        models.ForeignKey(
            TransformationRequest,
            on_delete=models.CASCADE,
            related_name="worker_logs",
            null=True,
        )
    )
    event_time = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=20)
    message = models.TextField()
    exception_type = models.CharField(max_length=255, blank=True)
    traceback = models.TextField(blank=True)

    class Meta:
        indexes = [models.Index(fields=["transform_request", "created_at"])]
