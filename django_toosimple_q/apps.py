from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules

from .logging import logger
from .registry import schedules, tasks


class DjangoToosimpleQConfig(AppConfig):
    name = "django_toosimple_q"
    label = "toosimpleq"

    def ready(self):
        # Autodicover tasks.py modules

        logger.info("Autodiscovering tasks.py...")
        autodiscover_modules("tasks")

        if len(schedules):
            schedules_names = ", ".join(schedules.keys())
            logger.info(f"Found {len(schedules)} schedules : {schedules_names}")
        else:
            logger.info("No schedules found")

        if len(tasks):
            tasks_names = ", ".join(tasks.keys())
            logger.info(f"Found {len(tasks)} tasks : {tasks_names}")
        else:
            logger.info("No schedules found")
