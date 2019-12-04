import time
import datetime
import signal

from django.core.management.base import BaseCommand
from django.utils.translation import ugettext as _
from django.utils.module_loading import autodiscover_modules

from ...registry import tasks, schedules
from ...models import Schedule, Task
from ...logging import logger


class Command(BaseCommand):

    help = _('Run tasks an schedules')

    def add_arguments(self, parser):
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument('--once', action='store_true', help='run once then exit')
        mode.add_argument('--until_done', action='store_true', help='run until no tasks are available then exit')

        schedules = parser.add_mutually_exclusive_group()
        schedules.add_argument('--no_recreate', action='store_true', help='do not (re)populate the schedule table')
        schedules.add_argument('--recreate_only', action='store_true', help='populates the schedule table then exit')

    def handle(self, *args, **options):

        # Handle SIGTERM and SIGINT (default_int_handler raises KeyboardInterrupt)
        # see https://stackoverflow.com/a/40785230
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.signal(signal.SIGTERM, signal.default_int_handler)

        logger.info("Autodiscovering tasks.py...")
        autodiscover_modules("tasks")

        logger.info("Loaded {} schedules : {}".format(len(schedules), list(schedules.keys())))
        logger.info("Loaded {} tasks : {}".format(len(tasks), list(tasks.keys())))

        if not options['no_recreate']:
            self.create_schedules()

        if options['recreate_only']:
            logger.info("Exiting because --recreate_only was passed")
            return

        logger.info(f"Starting queue...")
        last_run = datetime.datetime.now()
        while True:
            did_something = self.tick()

            if options['once']:
                logger.info("Exiting loop because --once was passed")
                break

            if options['until_done'] and not did_something:
                logger.info("Exiting loop because --until_done was passed")
                break

            if not options['until_done']:
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
