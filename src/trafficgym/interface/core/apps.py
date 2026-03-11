from django.apps import AppConfig


class CoreConfig(AppConfig):
    name = "trafficgym.interface.core"

    def ready(self) -> None:
        import trafficgym.interface.core.signals  # noqa
