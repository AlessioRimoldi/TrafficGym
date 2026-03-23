from django.http import HttpRequest, HttpResponse, StreamingHttpResponse
from django.shortcuts import render, get_object_or_404, redirect
from .models import RunRequest, Artefact, SubscriptionLogEntry, Scenario, Experiment
from django.views.decorators.http import require_POST
from django.db.models import Min, Max, Count
from django.core.paginator import Paginator
from django.conf import settings
from django.http import FileResponse, Http404
from pathlib import Path
from django.contrib.admin.views.decorators import staff_member_required

import json


def index(_: HttpRequest) -> HttpResponse:
    return HttpResponse("Help world")


def run_requests_list_view(request: HttpRequest) -> HttpResponse:
    run_requests_list = RunRequest.objects.all()
    context = {"run_requests_list": run_requests_list}

    return render(request, "core/run_requests_list.html", context)

@staff_member_required
def create_run_modal(request: HttpRequest) -> HttpResponse:
    scenarios = Scenario.objects.only("id", "name").order_by("name")
    experiments = Experiment.objects.only("sha256", "name", "version").order_by("name", "version")

    context = {"scenarios": scenarios, "experiments": experiments}

    return render(request, "core/create_run_modal.html", context)

@require_POST
def create_run_request(request: HttpRequest) -> HttpResponse:
    scenario_id = request.POST.get("scenario")
    experiment_id = request.POST.get("experiment")
    simulation_parameters = request.POST.get("simulation_parameters", "{}")

    scenario = get_object_or_404(Scenario, pk=scenario_id)
    experiment = get_object_or_404(Experiment, pk=experiment_id)

    run = RunRequest(
        scenario=scenario,
        experiment=experiment,
        simulation_parameters=json.loads(simulation_parameters)
    )

    run.save()

    return redirect("run_request_detail", pk=run.pk)



def run_request_detail_view(request: HttpRequest, pk: str) -> HttpResponse:
    run_request = get_object_or_404(RunRequest, pk=pk)
    subscription_data = (
        run_request.subscription_logs.values("subscription_fingerprint")
        .annotate(
            first_step=Min("simulation_step"), last_step=Max("simulation_step"), count=Count("*")
        )
        .order_by("subscription_fingerprint")
    )

    level_filter = request.GET.get("level")

    logs_qs = run_request.worker_logs.all()

    if level_filter:
        logs_qs = logs_qs.filter(level=level_filter)

    logs_qs = logs_qs.order_by("-event_time")

    paginator = Paginator(logs_qs, 25)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    context = {
        "run_request": run_request,
        "subscription_data": subscription_data,
        "worker_log_count": logs_qs.count(),
        "worker_logs": page_obj,
        "level_filter": level_filter,
    }

    return render(request, "core/run_request_detail.html", context)


def artefacts_list_view(request: HttpRequest) -> HttpResponse:
    artefacts_list = Artefact.objects.all()
    context = {"artefacts_list": artefacts_list}

    return render(request, "core/artefacts_list.html", context)


def artefact_detail_view(request: HttpRequest, pk: str) -> HttpResponse:
    artefact = get_object_or_404(Artefact, pk=pk)
    context = {"artefact": artefact}

    return render(request, "core/artefact_detail.html", context)


def media_view(_: HttpRequest, filepath: Path) -> StreamingHttpResponse:
    # Need to sanitise path
    system_path = Path(settings.MEDIA_ROOT) / filepath

    if not system_path.exists():
        raise Http404("File not Found")

    return FileResponse(open(system_path, "rb"), content_type="text/plain")


def subscription_view(
    request: HttpRequest, pk: str, fingerprint: str
) -> HttpResponse:
    view_type = request.GET.get("view", "table")

    run_request = get_object_or_404(RunRequest, pk=pk)

    subscription_data = (
        run_request.subscription_logs.filter(
            subscription_fingerprint=fingerprint
        )
        .order_by("simulation_time")
        .values("simulation_time", "payload")
    )

    if view_type == "graph":
        template_name = "core/subscription_plot.html"

        timestamps = []
        values = []

        for entry in subscription_data:
            timestamps.append(entry["simulation_time"])
            values.append(float(entry["payload"]))

        context = {
            "timestamps": timestamps,
            "values": values,
            "fingerprint": fingerprint,
            "run_request": run_request,
        }
    else:
        template_name = "core/subscription_table.html"

        paginator = Paginator(subscription_data, 100)
        page_number = request.GET.get("page", 1)
        page_obj = paginator.get_page(page_number)

        context = {
            "subscription_data": page_obj,
            "fingerprint": fingerprint,
            "run_request": run_request,
        }

    return render(request, template_name, context)
