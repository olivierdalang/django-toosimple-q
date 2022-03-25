import logging
import signal
import time

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.utils.translation import ugettext as _

from ...logging import logger
from ...models import TaskExec
from ...registry import dump_registry, schedules, tasks


class Command(BaseCommand):

    help = _("Run tasks an schedules")

    def add_arguments(self, parser):
        queue = parser.add_mutually_exclusive_group()
        queue.add_argument(
            "--queue",
            action="append",
            help="which queue to run (can be used several times, all queues are run if not provided)",
        )
        queue.add_argument(
            "--exclude_queue",
            action="append",
            help="which queue not to run (can be used several times, all queues are run if not provided)",
        )

        mode = parser.add_mutually_exclusive_group()
        mode.add_argument(
            "--once",
            action="store_true",
            help="run once then exit (useful for debugging)",
        )
        mode.add_argument(
            "--until_done",
            action="store_true",
            help="run until no tasks are available then exit (useful for debugging)",
        )

        parser.add_argument(
            "--tick",
            default=10.0,
            type=float,
            help="frequency in seconds at which the database is checked for new tasks/schedules",
        )

    def handle(self, *args, **options):

        if int(options["verbosity"]) > 1:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        # Handle SIGTERM and SIGINT (default_int_handler raises KeyboardInterrupt)
        # see https://stackoverflow.com/a/40785230
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.signal(signal.SIGTERM, signal.default_int_handler)

        logger.info("Starting worker")
        dump_registry()

        self.queues = options["queue"]
        self.excluded_queues = options["exclude_queue"]

        if self.queues:
            logger.info(f"Starting queues {self.queues}...")
        elif self.excluded_queues:
            logger.info(f"Starting queues except {self.excluded_queues}...")
        else:
            logger.info(f"Starting all queues...")

        last_run = timezone.now()
        while True:
            did_something = self.tick()

            if options["once"]:
                logger.info("Exiting loop because --once was passed")
                break

            if options["until_done"] and not did_something:
                logger.info("Exiting loop because --until_done was passed")
                break

            if not did_something:
                # wait for next tick
                dt = (timezone.now() - last_run).total_seconds()
                time.sleep(max(0, options["tick"] - dt))

            last_run = timezone.now()

    def tick(self):
        """Returns True if something happened (so you can loop for testing)"""

        did_something = False

        logger.debug(f"Checking schedules...")
        for schedule in schedules.values():
            did_something |= schedule.execute()

        logger.debug(f"Waking up tasks...")
        TaskExec.objects.filter(state=TaskExec.SLEEPING).filter(
            due__lte=timezone.now()
        ).update(state=TaskExec.QUEUED)

        logger.debug(f"Checking tasks...")
        tasks_execs = TaskExec.objects.filter(state=TaskExec.QUEUED)
        if self.queues:
            tasks_execs = tasks_execs.filter(queue__in=self.queues)
        if self.excluded_queues:
            tasks_execs = tasks_execs.exclude(queue__in=self.excluded_queues)
        task_exec = tasks_execs.order_by("-priority", "created").first()
        if task_exec:
            # We ensure the task is in the registry
            if task_exec.task_name in tasks:
                task = tasks[task_exec.task_name]
                did_something |= task.execute(task_exec)
            else:
                # If not, we set it as invalid
                task_exec.state = TaskExec.INVALID
                task_exec.save()
                logger.warning(f"Found invalid task execution: {task_exec}")
                did_something = True

        return did_something
