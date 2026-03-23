from typing import Any
from django.contrib import admin
from django.http import HttpRequest
from django.db.models import QuerySet

# Register your models here.

from .models import (
    RunRequest,
    Scenario,
    Artefact,
    Experiment,
    WorkerLogEntryRunRequest,
    WorkerLogEntryRunExecution,
    RPCLogEntry,
    SubscriptionLogEntry,
    TelemetryLogEntry,
)


def duplicate_run_request(
    model_admin: admin.ModelAdmin[RunRequest],
    request: HttpRequest,
    queryset: QuerySet[RunRequest],
) -> None:
    for run_request in queryset:
        RunRequest.objects.create(
            scenario=run_request.scenario, experiment=run_request.experiment
        )
    model_admin.message_user(
        request, f"{queryset.count()} run request(s) duplicated."
    )


# RunRequestOrderByDateCompatibleModels = WorkerLogEntry | RPCLogEntry | SubscriptionLogEntry | TelemetryLogEntry
# class RunRequestOrderByDateFilter(admin.SimpleListFilter):
#     title = 'run_id'
#     parameter_name = 'run_id'

#     def lookups(self, request: HttpRequest, model_admin: admin.ModelAdmin[RunRequestOrderByDateCompatibleModels]) -> list[tuple[Any, str]]:
#         qs = model_admin.get_queryset(request).select_related("run_request").order_by("-run_request__created_at")
#         return [(str(d), str(d)) for d in qs[:50]]


#     def queryset(self, _: HttpRequest, queryset: QuerySet[RunRequestOrderByDateCompatibleModels]) -> QuerySet[RunRequestOrderByDateCompatibleModels]:
#         if self.value():
#             return queryset.filter(id=self.value())
#         return queryset


@admin.register(RunRequest)
class RunRequestAdmin(admin.ModelAdmin[RunRequest]):
    list_display = ("id", "status", "created_at", "started_at", "finished_at")
    list_filter = ("status",)

    actions = [duplicate_run_request]


admin.site.register(Artefact)
admin.site.register(Scenario)
admin.site.register(Experiment)


@admin.register(WorkerLogEntryRunExecution)
class WorkerLogEntryRunExecutionAdmin(
    admin.ModelAdmin[WorkerLogEntryRunExecution]
):
    list_display = ("run_execution__id", "level", "event_time", "message")
    list_filter = ("run_execution__id", "level", "event_time")


@admin.register(WorkerLogEntryRunRequest)
class WorkerLogEntryRunRequestAdmin(admin.ModelAdmin[WorkerLogEntryRunRequest]):
    list_display = ("run_request__id", "level", "event_time", "message")
    list_filter = ("run_request__id", "level", "event_time")


@admin.register(RPCLogEntry)
class RPCLogEntryAdmin(admin.ModelAdmin[RPCLogEntry]):
    list_display = (
        "run_execution__id",
        "event_time",
        "rpc_name",
        "direction",
        "rpc_call_id",
    )
    list_filter = ("run_execution__id", "event_time", "rpc_name", "direction")


@admin.register(SubscriptionLogEntry)
class SubscriptionLogEntryAdmin(admin.ModelAdmin[SubscriptionLogEntry]):
    list_display = (
        "run_execution__id",
        "simulation_step",
        "subscription_fingerprint",
        "payload",
    )
    list_filter = (
        "run_execution__id",
        "event_time",
        "subscription_fingerprint",
    )


@admin.register(TelemetryLogEntry)
class TelemetryLogEntryAdmin(admin.ModelAdmin[TelemetryLogEntry]):
    list_display = (
        "run_execution__id",
        "simulation_step",
        "telemetry_name",
        "payload",
    )
    list_filter = ("run_execution__id", "event_time", "telemetry_name")
