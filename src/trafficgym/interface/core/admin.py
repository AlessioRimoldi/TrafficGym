from django.contrib import admin
from django.http import HttpRequest
from django.db.models import QuerySet

# Register your models here.

from .models import (
    RunRequest,
    Scenario,
    Artefact,
    Experiment,
    WorkerLogEntry,
    RPCLogEntry,
    SubscriptionLogEntry,
    TelemetryLogEntry,
)


def duplicate_run_request(
    modeladmin: admin.ModelAdmin[RunRequest],
    request: HttpRequest,
    queryset: QuerySet[RunRequest],
) -> None:
    for run_request in queryset:
        RunRequest.objects.create(
            scenario=run_request.scenario, experiment=run_request.experiment
        )
    modeladmin.message_user(
        request, f"{queryset.count()} run request(s) duplicated."
    )


@admin.register(RunRequest)
class RunRequestAdmin(admin.ModelAdmin[RunRequest]):
    list_display = ("id", "status", "created_at", "started_at", "finished_at")
    list_filter = ("status",)

    actions = [duplicate_run_request]


admin.site.register(Artefact)
admin.site.register(Scenario)
admin.site.register(Experiment)


@admin.register(WorkerLogEntry)
class WorkerLogEntryAdmin(admin.ModelAdmin[WorkerLogEntry]):
    list_display = ("run_request__id", "level", "event_time", "message")
    list_filter = ("run_request__id", "level", "event_time")


@admin.register(RPCLogEntry)
class RPCLogEntryAdmin(admin.ModelAdmin[RPCLogEntry]):
    list_display = (
        "run_request__id",
        "event_time",
        "rpc_name",
        "direction",
        "rpc_call_id",
    )
    list_filter = ("run_request__id", "event_time", "rpc_name", "direction")


@admin.register(SubscriptionLogEntry)
class SubscriptionLogEntryAdmin(admin.ModelAdmin[SubscriptionLogEntry]):
    list_display = (
        "run_request__id",
        "event_time",
        "subscription_fingerprint",
        "payload",
    )
    list_filter = ("run_request_id", "event_time", "subscription_fingerprint")


@admin.register(TelemetryLogEntry)
class TelemetryLogEntryAdmin(admin.ModelAdmin[TelemetryLogEntry]):
    list_display = (
        "run_request__id",
        "event_time",
        "telemetry_name",
        "payload",
    )
    list_filter = ("run_request__id", "event_time", "telemetry_name")
