from django.urls import path
from . import views

urlpatterns = [
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
]
