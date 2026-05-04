from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseBadRequest,
    StreamingHttpResponse,
    JsonResponse,
)
from django.shortcuts import (
    render,
    get_object_or_404,
    redirect,
)
from .models import (
    RunRequest,
    RunExecution,
    Artefact,
    SubscriptionLogEntry,
    Scenario,
    Experiment,
    TransformationRequest,
    TransformationInput,
)
from .forms import ScenarioForm, ExperimentForm, ArtefactForm
from django.urls import reverse
from django.views.decorators.http import require_POST
from django.db.models import Min, Max, Count, Avg, Sum, F
from django.core.paginator import Paginator
from django.conf import settings
from django.http import FileResponse, Http404
from django.contrib import messages
from pathlib import Path
from collections import defaultdict
from typing import DefaultDict, Set, TypedDict, cast, Literal
from .light_grpc_client import light_engine_client
from trafficgym.api.engine_pb2 import ListTransformationsRequest, InputType
from grpc import RpcError

import json
import mimetypes

aggregation_map = {
    "sum": Sum("payload"),
    "avg": Avg("payload"),
    "max": Max("payload"),
    "min": Min("payload"),
}

class InputSpec(TypedDict):
    name: str
    type: Literal["FILE", "JSON"]
    required: bool


class TransformationSpec(TypedDict):
    key: str
    inputs: list[InputSpec]
    outputs: list[str]
    docstring: str


def index(_: HttpRequest) -> HttpResponse:
    return HttpResponse("Help world")


def run_requests_list_view(request: HttpRequest) -> HttpResponse:
    run_requests_list = RunRequest.objects.all()
    context = {"run_requests_list": run_requests_list}

    return render(request, "core/run_requests_list.html", context)


# @staff_member_required
def create_run_modal(request: HttpRequest) -> HttpResponse:
    scenarios = Scenario.objects.only("id", "name").order_by("name")
    experiments = Experiment.objects.only("sha256", "name", "version").order_by(
        "name", "version"
    )

    context = {"scenarios": scenarios, "experiments": experiments}

    return render(request, "core/create_run_modal.html", context)


def select_run_modal(request: HttpRequest) -> HttpResponse:
    existing_run_ids = request.GET.get("existing_run_ids", "").split(",")
    other_runs = (
        RunRequest.objects.exclude(id__in=existing_run_ids)
        .order_by("-created_at")
        .values(
            "id",
            scenario_name=F("scenario__name"),
            experiment_name=F("experiment__name"),
            experiment_version=F("experiment__version"),
        )
    )

    context = {"available_runs": list(other_runs)}

    return render(request, "core/select_run_modal.html", context)


@require_POST
def create_run_request(request: HttpRequest) -> HttpResponse:
    scenario_id = request.POST.get("scenario")
    experiment_id = request.POST.get("experiment")
    rerun_count = int(request.POST.get("rerun_count", "1"))
    simulation_parameters = request.POST.get("simulation_parameters", "{}")

    scenario = get_object_or_404(Scenario, pk=scenario_id)
    experiment = get_object_or_404(Experiment, pk=experiment_id)

    run = RunRequest(
        scenario=scenario,
        experiment=experiment,
        rerun_count=rerun_count,
        simulation_parameters=json.loads(simulation_parameters),
    )

    run.save()

    return redirect("run_request_detail", pk=run.pk)


def run_request_detail_view(request: HttpRequest, pk: str) -> HttpResponse:
    run_request = get_object_or_404(RunRequest, pk=pk)

    level_filter = request.GET.get("level")

    logs_qs = run_request.worker_logs
    executions_qs = run_request.executions.all().order_by("-created_at")

    if level_filter:
        logs_qs = logs_qs.filter(level=level_filter)

    logs_qs = logs_qs.order_by("-event_time")

    logs_paginator = Paginator(logs_qs, 25)
    logs_page_number = request.GET.get("logs_page")
    logs_page_obj = logs_paginator.get_page(logs_page_number)

    executions_paginator = Paginator(executions_qs, 10)
    executions_page_number = request.GET.get("executions_page")
    executions_page_obj = executions_paginator.get_page(executions_page_number)

    logs = SubscriptionLogEntry.objects.filter(
        run_execution__run_request=run_request
    )

    fingerprint_rows = logs.values(
        "run_execution__id", "subscription_fingerprint"
    ).distinct()

    fingerprints_per_execution: DefaultDict[str, Set[str]] = defaultdict(set)

    for fingerprint_row in fingerprint_rows:
        fingerprints_per_execution[
            str(fingerprint_row["run_execution__id"])
        ].add(fingerprint_row["subscription_fingerprint"])

    fingerprint_sets = list(fingerprints_per_execution.values())

    if fingerprint_sets:
        all_fingerprints: Set[str] = set.union(*fingerprint_sets)
        common_fingerprints: Set[str] = set.intersection(*fingerprint_sets)

    else:
        all_fingerprints = set()
        common_fingerprints = set()

    # missing_by_execution = {
    #     exec_id: all_fingerprints - fps
    #     for exec_id, fps in fingerprints_per_execution.items()
    # }

    aggregated_subscriptions = (
        logs.values("subscription_fingerprint")
        .annotate(
            execution_count=Count("run_execution", distinct=True),
            first_step=Min("simulation_step"),
            last_step=Max("simulation_step"),
            # avg_value=Avg("value"),  # example numeric aggregation
        )
        .order_by("subscription_fingerprint")
    )

    analytics_links: dict[str, str] = {
        row["subscription_fingerprint"]: json.dumps(
            {
                "runs": {
                    str(pk): [
                        {
                            "subscription": row["subscription_fingerprint"],
                            "aggMode": "avg",
                            "runExecutions": [""],
                        }
                    ]
                }
            },
            separators=(",", ":"),
        )
        for row in aggregated_subscriptions
    }

    context = {
        "run_request": run_request,
        "worker_log_count": logs_qs.count(),
        "worker_logs": logs_page_obj,
        "executions_count": executions_qs.count(),
        "executions": executions_page_obj,
        "level_filter": level_filter,
        "aggregated_subscriptions_count": aggregated_subscriptions.count(),
        "aggregated_subscriptions": aggregated_subscriptions,
        "has_missing_subs": all_fingerprints != common_fingerprints,
        "analytics_links": analytics_links,
        # "common_fingerprints": common_fingerprints,
    }

    return render(request, "core/run_request_detail.html", context)


def run_execution_detail_view(request: HttpRequest, pk: str) -> HttpResponse:
    run_execution = get_object_or_404(RunExecution, pk=pk)
    subscription_data = (
        run_execution.subscription_logs.values("subscription_fingerprint")
        .annotate(
            first_step=Min("simulation_step"),
            last_step=Max("simulation_step"),
            count=Count("*"),
        )
        .order_by("subscription_fingerprint")
    )

    level_filter = request.GET.get("level")

    logs_qs = run_execution.worker_logs.all()

    if level_filter:
        logs_qs = logs_qs.filter(level=level_filter)

    logs_qs = logs_qs.order_by("-event_time")

    paginator = Paginator(logs_qs, 25)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    analytics_links: dict[str, str] = {
        row["subscription_fingerprint"]: json.dumps(
            {
                "runs": {
                    str(cast(RunRequest, run_execution.run_request).id): [
                        {
                            "subscription": row["subscription_fingerprint"],
                            "aggMode": "",
                            "runExecutions": [str(pk)],
                        }
                    ]
                }
            },
            separators=(",", ":"),
        )
        for row in subscription_data
    }

    context = {
        "run_execution": run_execution,
        "subscription_data": subscription_data,
        "worker_log_count": logs_qs.count(),
        "worker_logs": page_obj,
        "level_filter": level_filter,
        "analytics_links": analytics_links,
    }

    return render(request, "core/run_execution_detail.html", context)


def artefacts_list_view(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        form = ArtefactForm(request.POST, request.FILES)
        if not request.FILES:
            messages.error(
                request,
                f"Please select at least one file to upload",
                extra_tags="danger",
            )

        if form.is_valid():
            created, reused = form.save()

            if created:
                messages.success(
                    request, f"Created {len(created)} new artefact(s)."
                )
            if reused:
                messages.info(
                    request,
                    f"{len(reused)} artefact(s) already existed and were reused.",
                )
        else:
            messages.error(
                request,
                f"Issue processing form data: {form.errors}",
                extra_tags="danger",
            )

        return redirect("artefacts_list")
    else:
        form = ArtefactForm()

    artefacts_list = Artefact.objects.prefetch_related(
        "scenarios", "experiments"
    ).all()
    context = {"artefacts": artefacts_list, "form": form}

    return render(request, "core/artefacts_list.html", context)


def artefact_detail_view(request: HttpRequest, pk: str) -> HttpResponse:
    artefact = get_object_or_404(
        Artefact.objects.prefetch_related(
            # provenance: outputs → transformation → metadata
            "provenance__transformation_request",

            # derivatives: inputs → transformation → outputs → artefacts
            "derivatives__transformation_request__output_bindings__artefact",
        ),
        pk=pk,
    )

    return render(request, "core/artefact_detail.html", {"artefact": artefact})

def media_view(_: HttpRequest, filepath: str) -> StreamingHttpResponse:
    media_root = Path(settings.MEDIA_ROOT).resolve()

    system_path = (media_root / filepath).resolve()

    if media_root not in system_path.parents and system_path != media_root:
        raise Http404("Invalid path")

    if not system_path.exists():
        raise Http404("File not found")

    artefact = Artefact.objects.get(file=str(filepath))

    mime = mimetypes.guess_type(str(artefact.original_name))[0]

    return FileResponse(
        open(system_path, "rb"),
        # content_type=mime or "application/octet-stream",
        content_type=mime or "text/plain",
    )

def subscription_data(request: HttpRequest, pk: str) -> HttpResponse:
    aggregation_function = request.GET.get("agg_mode")
    run_executions = request.GET.getlist("run_execution")
    fingerprints = request.GET.getlist("fingerprint")

    if run_executions and run_executions[0] != "":
        if not RunExecution.objects.filter(id__in=run_executions).exists():
            raise Http404()

        base_qs = SubscriptionLogEntry.objects.filter(
            run_execution__id__in=run_executions
        )
    else:
        run_request = get_object_or_404(RunRequest, pk=pk)
        base_qs = SubscriptionLogEntry.objects.filter(
            run_execution__run_request=run_request
        )

    if fingerprints:
        base_qs = base_qs.filter(subscription_fingerprint__in=fingerprints)

    class AggreagatedRow(TypedDict):
        sum: float
        min: float
        max: float
        count: int
        run_execution_ids: Set[str]

    aggregated_data = []
    run_execution_data = []
    warnings = []

    if aggregation_function in ["sum", "avg", "max", "min"]:
        # Initialize aggregation per fingerprint
        agg_temp: dict[tuple[float, str], AggreagatedRow] = {}
        for e in base_qs.values(
            "simulation_time",
            "subscription_fingerprint",
            "run_execution__id",
            "payload",
        ):
            sim_time = e["simulation_time"]
            fp = e["subscription_fingerprint"]
            try:
                val = float(e["payload"])
            except (ValueError, TypeError):
                continue  # skip non-numeric

            key = (sim_time, fp)
            if key not in agg_temp:
                agg_temp[key] = {
                    "sum": 0.0,
                    "count": 0,
                    "min": val,
                    "max": val,
                    "run_execution_ids": set(),
                }

            entry = agg_temp[key]
            entry["sum"] += val
            entry["count"] += 1
            entry["min"] = min(entry["min"], val)
            entry["max"] = max(entry["max"], val)

            entry["run_execution_ids"].add(str(e["run_execution__id"]))

        if not agg_temp:
            warnings.append(
                f"Aggregation '{aggregation_function}' returned no data. "
                f"No numeric values found for the selected fingerprints ({', '.join(fingerprints) or 'all'}) "
                f"and run request {pk}, run executions ({', '.join(run_executions) or 'all'})."
            )

        for (sim_time, fp), entry in agg_temp.items():
            if aggregation_function == "sum":
                agg_value = entry["sum"]
            elif aggregation_function == "avg":
                agg_value = (
                    entry["sum"] / entry["count"] if entry["count"] > 0 else 0.0
                )
            elif aggregation_function == "min":
                agg_value = entry["min"]
            elif aggregation_function == "max":
                agg_value = entry["max"]
            else:
                agg_value = None

            aggregated_data.append(
                {
                    "run_execution": sorted(entry["run_execution_ids"]),
                    "time": sim_time,
                    "fingerprint": fp,
                    "value": agg_value,
                }
            )

    else:
        run_execution_data = [
            {
                "run_execution": [str(e.run_execution.id)],
                "time": cast(float, e.simulation_time),
                "fingerprint": cast(str, e.subscription_fingerprint),
                "value": e.payload,
            }
            for e in base_qs
        ]

    return JsonResponse(
        {
            "run_execution_data": run_execution_data,
            "aggregated_data": aggregated_data,
            "warnings": warnings,
        }
    )


def analytics_overview_view(request: HttpRequest) -> HttpResponse:
    latest_rr = RunRequest.objects.order_by("-created_at").only("pk").first()

    if latest_rr is None:
        return render(request, "core/analytics_empty.html")

    url = reverse("analytics")

    return redirect(f"{url}?run_request={latest_rr.id}")


def analytics_run_execution_view(_: HttpRequest, pk: str) -> HttpResponse:
    run_execution = get_object_or_404(RunExecution, pk=pk)

    url = reverse(
        "analytics_run_request", kwargs={"pk": run_execution.run_request}
    )

    return redirect(f"{url}?run_execution={run_execution.pk}")


def analytics_run_request_view(request: HttpRequest) -> HttpResponse:
    run_request_id_params = list(set(request.GET.getlist("run_request")))

    run_requests = RunRequest.objects.filter(
        id__in=run_request_id_params
    ).prefetch_related("executions")

    logs = SubscriptionLogEntry.objects.filter(
        run_execution__run_request__id__in=run_request_id_params
    )

    fingerprint_rows = logs.values(
        "run_execution__id",
        "run_execution__run_request__id",
        "subscription_fingerprint",
    ).distinct()

    fingerprints_per_run: DefaultDict[str, DefaultDict[str, Set[str]]] = (
        defaultdict(lambda: defaultdict(set))
    )

    for row in fingerprint_rows:
        rid = str(row["run_execution__run_request__id"])
        exec_id = str(row["run_execution__id"])
        fp = row["subscription_fingerprint"]

        fingerprints_per_run[rid][exec_id].add(fp)

    for rid, executions in fingerprints_per_run.items():
        fingerprint_sets = list(executions.values())

        if fingerprint_sets:
            all_fingerprints: Set[str] = set.union(*fingerprint_sets)
            common_fingerprints: Set[str] = set.intersection(*fingerprint_sets)

        else:
            all_fingerprints = set()
            common_fingerprints = set()

        if all_fingerprints != common_fingerprints:
            for exec_id, fps in executions.items():
                missing = all_fingerprints - fps
                if missing:
                    messages.warning(
                        request,
                        f"Run Request {rid} — Run Execution {exec_id} "
                        f"is missing data for the following fingerprint(s): "
                        f"{", ".join(sorted(missing))}",
                    )

    aggregated_subscriptions = (
        logs.values(
            "run_execution__run_request__id", "subscription_fingerprint"
        )
        .annotate(
            execution_count=Count("run_execution", distinct=True),
            first_step=Min("simulation_step"),
            last_step=Max("simulation_step"),
            # avg_value=Avg("value"),  # example numeric aggregation
        )
        .order_by("run_execution__run_request__id", "subscription_fingerprint")
    )

    aggregated_subscriptions_per_run = defaultdict(list)

    for sub in aggregated_subscriptions:
        run_id = sub["run_execution__run_request__id"]
        aggregated_subscriptions_per_run[run_id].append(sub)

    context = {
        "run_requests": run_requests,
        "aggregated_susbcriptions_per_run": aggregated_subscriptions_per_run,
        "aggregation_functions": aggregation_map.keys(),
    }

    return render(request, "core/analytics.html", context)


def subscription_view(
    request: HttpRequest, pk: str, fingerprint: str
) -> HttpResponse:
    view_type = request.GET.get("view", "table")

    run_execution = get_object_or_404(RunExecution, pk=pk)

    subscription_data = (
        run_execution.subscription_logs.filter(
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
            "run_execution": run_execution,
        }
    else:
        template_name = "core/subscription_table.html"

        paginator = Paginator(subscription_data, 100)
        page_number = request.GET.get("page", 1)
        page_obj = paginator.get_page(page_number)

        context = {
            "subscription_data": page_obj,
            "fingerprint": fingerprint,
            "run_execution": run_execution,
        }

    return render(request, template_name, context)


def scenario_overview(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        form = ScenarioForm(request.POST, request.FILES)

        if form.is_valid():
            scenario, created, reused = form.save()

            if created:
                messages.success(
                    request, f"Created {len(created)} new artefact(s)."
                )
            if reused:
                messages.info(
                    request,
                    f"{len(reused)} artefact(s) already existed and were reused.",
                )
        else:
            messages.error(
                request,
                f"Issue processing form data: {form.errors}",
                extra_tags="danger",
            )

        return redirect("scenario_overview")
    else:
        form = ScenarioForm()

    scenarios = (
        Scenario.objects.prefetch_related("run_requests", "artefacts")
        .all()
        .order_by("-created_at")
    )

    context = {"scenarios": scenarios, "form": form}

    return render(request, "core/scenario_overview.html", context)


def scenario_detail(request: HttpRequest, pk: str) -> HttpResponse:
    scenario = get_object_or_404(Scenario, pk)

    context = {"scneario": scenario}
    return render(request, "core/scenario_detail.html", context)


def experiments_overview(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        form = ExperimentForm(request.POST, request.FILES)

        if not request.FILES:
            messages.error(
                request,
                f"Please select at least one file to upload",
                extra_tags="danger",
            )
        if form.is_valid():
            experiment, created, reused = form.save()

            if created:
                messages.success(
                    request, f"Created {len(created)} new artefact(s)."
                )
            if reused:
                messages.info(
                    request,
                    f"{len(reused)} artefact(s) already existed and were reused.",
                )

        else:
            messages.error(
                request,
                f"Issue processing form data: {form.errors}",
                extra_tags="danger",
            )

        return redirect("experiment_overview")
    else:
        form = ExperimentForm()

    experiments = Experiment.objects.prefetch_related("run_requests").all()

    context = {"experiments": experiments, "form": form}

    return render(request, "core/experiment_overview.html", context)


def transformation_requests_list_view(request: HttpRequest) -> HttpResponse:
    transformation_requests = TransformationRequest.objects.all().order_by("-created_at")

    context = {"transformation_requests": transformation_requests}
    return render(request, "core/transformation_requests_list.html", context)


def transformation_request_detail_view(
    request: HttpRequest, pk: str
) -> HttpResponse:
    transformation_request = get_object_or_404(TransformationRequest, pk=pk)

    level_filter = request.GET.get("level")

    logs_qs = transformation_request.worker_logs

    if level_filter:
        logs_qs = logs_qs.filter(level=level_filter)

    logs_qs = logs_qs.order_by("-event_time")

    logs_paginator = Paginator(logs_qs, 25)
    logs_page_number = request.GET.get("logs_page")
    logs_page_obj = logs_paginator.get_page(logs_page_number)

    context = {
        "transformation_request": transformation_request,
        "spec": transformation_request.spec_snapshot,
        "worker_log_count": logs_qs.count(),
        "worker_logs": logs_page_obj,
        "level_filter": level_filter,
    }

    return render(request, "core/transformation_request_detail.html", context)


def list_transformations_view(_: HttpRequest) -> JsonResponse:
    try:
        resp = light_engine_client.ListTransformations(
            ListTransformationsRequest()
        )
    except RpcError as e:
        return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse(
        {
            "transformations": [
                {
                    "key": t.key,
                    "inputs": [
                        {
                            "name": i.name,
                            "type": InputType.Name(i.type),
                            "required": i.required,
                        }
                        for i in t.inputs
                    ],
                    "outputs": list(t.outputs),
                    "docstring": t.docstring,
                }
                for t in resp.transformations
            ]
        }
    )


@require_POST
def create_transform_request(request: HttpRequest) -> HttpResponse:
    method = request.POST.get("method")

    if not method:
        return HttpResponseBadRequest("Missing Transformation method")

    raw_inputs = request.POST.get("inputs", "{}")
    raw_parameters = request.POST.get("simulation_parameters", "{}")
    raw_schema = request.POST.get("schema", "{}")
    try:
        inputs: dict[str, str] = json.loads(raw_inputs)
        parameters = json.loads(raw_parameters)
        schema: TransformationSpec = json.loads(raw_schema)
    except json.JSONDecodeError:
        return HttpResponseBadRequest("Invalid JSON")

    tr = TransformationRequest.objects.create(
        method=method,
        spec_snapshot=schema,
        parameters=parameters,
    )

    bindings = [
        TransformationInput(
            transformation_request=tr,
            artefact_id=artefact_sha256,
            input_name=input_name,
        )
        for input_name, artefact_sha256 in inputs.items()
    ]

    TransformationInput.objects.bulk_create(bindings)

    return redirect("transformation_request_detail", pk=tr.pk)
