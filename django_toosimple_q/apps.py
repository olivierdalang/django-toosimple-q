from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules

from .registry import tasks, schedules


class DjangoToosimpleQConfig(AppConfig):
    name = "django_toosimple_q"
    label = 'toosimpleq'

    def ready(self):
        # Autodicover tasks.py modules

        print("[toosimpleq] Autodiscovering tasks.py...")
        autodiscover_modules("tasks")
        print("[toosimpleq] found {} schedules : {}".format(len(schedules), ", ".join(schedules.keys())))
        print("[toosimpleq] found {} tasks : {}".format(len(tasks), ", ".join(tasks.keys())))
