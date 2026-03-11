from django.contrib import admin
from django.http import HttpRequest
from django.db.models import QuerySet

# Register your models here.

from .models import RunRequest, Scenario, Artefact, Experiment


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
