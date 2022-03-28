from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules

from .logging import logger, show_registry


class DjangoToosimpleQConfig(AppConfig):
    name = "django_toosimple_q"
    label = "toosimpleq"

    def ready(self):
        # Autodicover tasks.py modules

        logger.info("Autodiscovering tasks.py...")
        autodiscover_modules("tasks")

        show_registry()
