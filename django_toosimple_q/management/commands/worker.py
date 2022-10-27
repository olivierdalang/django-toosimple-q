import datetime
import logging
import os
import signal
import sys
import time

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Case, Value, When
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from ...logging import logger
from ...models import ScheduleExec, TaskExec, WorkerStatus
from ...registry import schedules_registry, tasks_registry


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

        parser.add_argument(
            "--label",
            default=r"worker-{pid}",
            help=r"the name of the worker to help identifying it ('{pid}' will be replaced by the process id)",
        )
        parser.add_argument(
            "--timeout",
            default=3600,
            type=float,
            help="the time in seconds after which this worker will be considered offline (set this to a value higher than the longest tasks this worker will execute)",
        )

    def handle(self, *args, **options):

        # Handle SIGTERM and SIGINT (default_int_handler raises KeyboardInterrupt)
        # see https://stackoverflow.com/a/40785230
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.signal(signal.SIGTERM, signal.default_int_handler)

        self.queues = options["queue"] or []
        self.excluded_queues = options["exclude_queue"] or []
        self.tick_duration = options["tick"]
        self.label = options["label"].replace(r"{pid}", str(os.getpid()))
        self.timeout = options["timeout"]

        logger.info(f"Starting worker '{self.label}'...")
        if self.queues:
            logger.info(f"Included queues: {self.queues}")
        elif self.excluded_queues:
            logger.info(f"Excluded queues: {self.excluded_queues}")

        self.verbosity = int(options["verbosity"])
        if self.verbosity == 0:
            logger.setLevel(logging.WARNING)
        elif self.verbosity == 1:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.DEBUG)

        # On startup, we report the worker went online
        self.worker_status, _ = WorkerStatus.objects.update_or_create(
            label=self.label,
            defaults={
                "started": timezone.now(),
                "stopped": None,
                "included_queues": self.queues,
                "excluded_queues": self.excluded_queues,
                "timeout": datetime.timedelta(seconds=self.timeout),
            },
        )

        try:
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
                    logger.debug(f"Waiting for next tick...")
                    dt = (timezone.now() - last_run).total_seconds()
                    time.sleep(max(0, self.tick_duration - dt))

                last_run = timezone.now()
        except (KeyboardInterrupt, SystemExit):
            logger.critical(f"Exiting at user's request")
            sys.exit(0)
        finally:
            # On exit (or other error), we report the worker went offline
            self.worker_status.stopped = timezone.now()
            self.worker_status.save()

    def tick(self):
        """Returns True if something happened (so you can loop for testing)"""

        logger.debug(f"Tick !")

        did_something = False

        logger.debug(f"Update status...")
        self.worker_status.last_tick = timezone.now()
        self.worker_status.save()

        logger.debug(f"Disabling orphaned schedules")
        with transaction.atomic():
            count = (
                ScheduleExec.objects.exclude(state=ScheduleExec.States.INVALID)
                .exclude(name__in=schedules_registry.keys())
                .update(state=ScheduleExec.States.INVALID)
            )
            if count > 0:
                logger.warning(f"Found {count} invalid schedules")

        logger.debug(f"Disabling orphaned tasks")
        with transaction.atomic():
            count = (
                TaskExec.objects.exclude(state=TaskExec.States.INVALID)
                .exclude(task_name__in=tasks_registry.keys())
                .update(state=TaskExec.States.INVALID)
            )
            if count > 0:
                logger.warning(f"Found {count} invalid tasks")

        logger.debug(f"Checking schedules")
        schedules_to_check = schedules_registry.for_queue(
            self.queues, self.excluded_queues
        )
        for schedule in schedules_to_check:
            did_something |= schedule.execute(self.tick_duration)

        logger.debug(f"Waking up tasks")
        TaskExec.objects.filter(state=TaskExec.States.SLEEPING).filter(
            due__lte=timezone.now()
        ).update(state=TaskExec.States.QUEUED)

        logger.debug(f"Checking tasks")
        # We compile an ordering clause from the registry
        order_by_priority_clause = Case(
            *[
                When(task_name=task.name, then=Value(-task.priority))
                for task in tasks_registry.values()
            ],
            default=Value(0),
        )
        tasks_to_check = tasks_registry.for_queue(self.queues, self.excluded_queues)
        tasks_execs = TaskExec.objects.filter(state=TaskExec.States.QUEUED)
        tasks_execs = tasks_execs.filter(task_name__in=[t.name for t in tasks_to_check])
        tasks_execs = tasks_execs.order_by(order_by_priority_clause, "due", "created")
        with transaction.atomic():
            task_exec = tasks_execs.select_for_update().first()
            if task_exec:
                logger.debug(f"{task_exec} is due !")
                task_exec.started = timezone.now()
                task_exec.state = TaskExec.States.PROCESSING
                task_exec.worker = self.worker_status
                task_exec.save()

        if task_exec:
            task = tasks_registry[task_exec.task_name]
            did_something |= task.execute(task_exec)

        return did_something
