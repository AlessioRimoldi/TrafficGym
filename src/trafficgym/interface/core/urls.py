from pathlib import Path
from django.urls import path, re_path
from django.http import HttpResponse
from django.shortcuts import redirect
from django.views.static import serve
from . import views

_DOCS_ROOT = Path(__file__).parents[4] / "docs" / "_build" / "html"

urlpatterns = [
    path("favicon.ico", lambda r: HttpResponse(status=204)),
    # path("", views.index, name="index"),
    path("", views.run_requests_list_view, name="run_requests_list"),
    path(
        "run_requests_list/",
        views.run_requests_list_view,
        name="run_requests_list",
    ),
    path(
        "run_request/<uuid:pk>/",
        views.run_request_detail_view,
        name="run_request_detail",
    ),
    path(
        "run_execution/<uuid:pk>/",
        views.run_execution_detail_view,
        name="run_execution_detail",
    ),
    path(
        "run_execution/<uuid:pk>/subscription_view/<str:fingerprint>",
        views.subscription_view,
        name="subscription_view",
    ),
    path("artefacts_list/", views.artefacts_list_view, name="artefacts_list"),
    path(
        "artefact/<str:pk>/",
        views.artefact_detail_view,
        name="artefact_detail",
    ),
    path("media/<path:filepath>", views.media_view, name="media_view"),
    path("create_run_modal", views.create_run_modal, name="create_run_modal"),
    path("select_run_modal", views.select_run_modal, name="select_run_modal"),
    path(
        "create_run_request",
        views.create_run_request,
        name="create_run_request",
    ),
    path(
        "analytics_overview",
        views.analytics_overview_view,
        name="analytics_overview",
    ),
    path(
        "analytics",
        views.analytics_run_request_view,
        name="analytics",
    ),
    path(
        "analytics_run_execution/<uuid:pk>",
        views.analytics_run_execution_view,
        name="analytics_run_execution",
    ),
    path(
        "analytics/subscriptions_data/<uuid:pk>",
        views.subscription_data,
        name="subscription_data",
    ),
    path(
        "scenario_overview",
        views.scenario_overview,
        name="scenario_overview",
    ),
    path(
        "experiment_overview",
        views.experiments_overview,
        name="experiment_overview",
    ),
    path(
        "transformation_requests_list",
        views.transformation_requests_list_view,
        name="transformation_requests_list",
    ),
    path(
        "transformation_request/<uuid:pk>",
        views.transformation_request_detail_view,
        name="transformation_request_detail",
    ),
    path(
        "api/blocks",
        views.blocks_registry_view,
        name="blocks_registry",
    ),
    path(
        "api/domains",
        views.domains_view,
        name="domains",
    ),
    path(
        "api/list_transformations",
        views.list_transformations_view,
        name="list_transformations",
    ),
    path(
        "api/scenarios/<uuid:scenario_id>/inspection",
        views.scenario_inspection_view,
        name="scenario_inspection",
    ),
    path(
        "api/scenarios/<uuid:scenario_id>/experiments",
        views.scenario_experiments_view,
        name="scenario_experiments",
    ),
    path(
        "api/scenarios/<uuid:scenario_id>/preview-url",
        views.scenario_preview_url_view,
        name="scenario_preview_url",
    ),
    path(
        "api/experiment_graphs/",
        views.save_experiment_graph,
        name="save_experiment_graph",
    ),
    path(
        "experiment_graph_builder_modal",
        views.experiment_graph_builder_modal,
        name="experiment_graph_builder_modal",
    ),
    path(
        "create_transformation_request",
        views.create_transform_request,
        name="create_transformation_request",
    ),
    path(
        "api/status-events/",
        views.status_events,
        name="status_events",
    ),
    path("docs/", lambda r: redirect("docs_files", path="index.html"), name="docs"),
    re_path(r"^docs/(?P<path>.+)$", serve, {"document_root": _DOCS_ROOT}, name="docs_files"),
]
