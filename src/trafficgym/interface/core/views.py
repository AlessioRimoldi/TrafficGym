from django.http import HttpRequest, HttpResponse, StreamingHttpResponse
from django.shortcuts import render, get_object_or_404

# Create your views here.

from .models import RunRequest, Artefact, SubscriptionLogEntry
from django.conf import settings
from django.http import FileResponse, Http404
from pathlib import Path


def index(request: HttpRequest) -> HttpResponse:
    return HttpResponse("Help world")


def run_requests_list_view(request: HttpRequest) -> HttpResponse:
    run_requests_list = RunRequest.objects.all()
    context = {"run_requests_list": run_requests_list}

    return render(request, "core/run_requests_list.html", context)


def run_request_detail_view(request: HttpRequest, pk: str) -> HttpResponse:
    run_request = get_object_or_404(RunRequest, pk=pk)
    context = {"run_request": run_request}

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


def subscription_plot(
    request: HttpRequest, pk: str, fingerprint: str
) -> HttpResponse:
    run_request = get_object_or_404(RunRequest, pk=pk)

    logs = (
        SubscriptionLogEntry.objects.filter(
            run_request=run_request, subscription_fingerprint=fingerprint
        )
        .order_by("event_time")
        .values("event_time", "payload")
    )

    timestamps = []
    values = []

    for entry in logs:
        timestamps.append(entry["event_time"].isoformat())
        values.append(float(entry["payload"]))

    context = {
        "timestamps": timestamps,
        "values": values,
        "fingerprint": fingerprint,
        "run_request": run_request,
    }

    return render(request, "core/subscription_plot.html", context)
