import time
import datetime
import signal
import logging

from django.core.management.base import BaseCommand
from django.utils.translation import ugettext as _

from ...registry import tasks, schedules
from ...models import Schedule, Task
from ...logging import logger


class Command(BaseCommand):

    help = _('Run tasks an schedules')

    def add_arguments(self, parser):
        queue = parser.add_mutually_exclusive_group()
        queue.add_argument('--queue', action='append', help="which queue to run (can be used several times, all queues are run if not provided)")
        queue.add_argument('--exclude_queue', action='append', help="which queue not to run (can be used several times, all queues are run if not provided)")

        mode = parser.add_mutually_exclusive_group()
        mode.add_argument('--once', action='store_true', help='run once then exit (useful for debugging)')
        mode.add_argument('--until_done', action='store_true', help='run until no tasks are available then exit (useful for debugging)')

        schedules = parser.add_mutually_exclusive_group()
        schedules.add_argument('--no_recreate', action='store_true', help='do not (re)populate the schedule table (useful for debugging)')
        schedules.add_argument('--recreate_only', action='store_true', help='populates the schedule table then exit (useful for debugging)')

        parser.add_argument('--tick', default=10.0, type=float, help="frequency in seconds at which the database is checked for new tasks/schedules")

    def handle(self, *args, **options):

        if int(options['verbosity']) > 1:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        # Handle SIGTERM and SIGINT (default_int_handler raises KeyboardInterrupt)
        # see https://stackoverflow.com/a/40785230
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.signal(signal.SIGTERM, signal.default_int_handler)

        logger.info("Loaded {} schedules : {}".format(len(schedules), ", ".join(schedules.keys())))
        logger.info("Loaded {} tasks : {}".format(len(tasks), ", ".join(tasks.keys())))

        if not options['no_recreate']:
            self.create_schedules()

        if options['recreate_only']:
            logger.info("Exiting because --recreate_only was passed")
            return

        self.queues = options['queue']
        self.excluded_queues = options['exclude_queue']

        if self.queues:
            logger.info(f"Starting queues {self.queues}...")
        elif self.excluded_queues:
            logger.info(f"Starting queues except {self.excluded_queues}...")
        else:
            logger.info(f"Starting all queues...")

        last_run = datetime.datetime.now()
        while True:
            did_something = self.tick()

            if options['once']:
                logger.info("Exiting loop because --once was passed")
                break

            if options['until_done'] and not did_something:
                logger.info("Exiting loop because --until_done was passed")
                break

            if not did_something:
                # wait for next tick
                dt = (datetime.datetime.now() - last_run).total_seconds()
                time.sleep(max(0, options['tick'] - dt))

            last_run = datetime.datetime.now()

    def create_schedules(self):
        ids = []
        for schedule_name, defaults in schedules.items():
            schedule, created = Schedule.objects.update_or_create(name=schedule_name, defaults=defaults)
            ids.append(schedule.id)
        Schedule.objects.exclude(id__in=ids).delete()

    def tick(self):
        """Returns True if something happened (so you can loop for testing)"""

        did_something = False

        logger.debug(f"Checking schedules...")
        for schedule in Schedule.objects.all():
            did_something |= schedule.execute()

        logger.debug(f"Checking tasks...")
        tasks = Task.objects.filter(state=Task.QUEUED)
        if self.queues:
            tasks = tasks.filter(queue__in=self.queues)
        if self.excluded_queues:
            tasks = tasks.exclude(queue__in=self.excluded_queues)
        task = tasks.order_by("-priority", "created").first()
        if task:
            did_something |= task.execute()

        return did_something
