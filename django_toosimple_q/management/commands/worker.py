import time
import datetime

from django.core.management.base import BaseCommand
from django.utils.translation import ugettext as _
from django.utils.module_loading import autodiscover_modules

from ...registry import tasks, schedules
from ...models import Schedule, Task
from ...logging import logger


class Command(BaseCommand):

    help = _('Run tasks an schedules')

    def handle(self, *args, **options):

        logger.info("Autodiscovering tasks.py...")
        autodiscover_modules("tasks")

        logger.info("Loaded {} schedules : {}".format(len(schedules), list(schedules.keys())))
        logger.info("Loaded {} tasks : {}".format(len(tasks), list(tasks.keys())))

        self.create_schedules()

        logger.info(f"Starting queue...")
        last_run = datetime.datetime.now()
        while True:
            self.tick()

            # wait for next tick
            dt = (datetime.datetime.now() - last_run).total_seconds()
            time.sleep(max(0, 10 - dt))

            last_run = datetime.datetime.now()

    def create_schedules(self):
        ids = []
        for schedule_name, kwargs in schedules.items():
            schedule, created = Schedule.objects.get_or_create(**kwargs)
            ids.append(schedule.id)
        Schedule.objects.exclude(id__in=ids).delete()

    def tick(self):
        """Returns True if something happened (so you can loop for testing)"""

        logger.info(f"tick...")

        did_something = False

        logger.info(f"Checking schedules...")
        for schedule in Schedule.objects.all():
            did_something |= schedule.execute()

        logger.info(f"Checking tasks...")
        task = Task.objects.filter(state=Task.QUEUED).order_by("-priority", "created").first()
        if task:
            did_something |= task.execute()

        return did_something
