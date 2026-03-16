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
        "run_request/<uuid:pk>/subscription/<str:fingerprint>",
        views.subscription_plot,
        name="subscription_plot",
    ),
    path("artefacts_list/", views.artefacts_list_view, name="artefacts_list"),
    path(
        "artefact/<str:pk>/",
        views.artefact_detail_view,
        name="artefact_detail",
    ),
    path("media/<path:filepath>", views.media_view, name="media_view"),
]
