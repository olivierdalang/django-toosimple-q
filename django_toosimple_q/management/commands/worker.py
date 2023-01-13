import datetime
import logging
import os
import signal
from traceback import format_exc

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Case, Value, When
from django.utils import autoreload, timezone

from ...logging import logger
from ...models import ScheduleExec, TaskExec, WorkerStatus
from ...registry import schedules_registry, tasks_registry
from ...tests.utils import FakeException


class Command(BaseCommand):
    help = "Run tasks and schedules"

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
            default=60 * 5,
            type=float,
            help="the time in seconds after which this worker will be considered offline (set this to a value higher than the longest tasks this worker will execute)",
        )
        parser.add_argument(
            "--reload",
            choices=["default", "always", "never"],
            default="default",
            help="watch for changes (by default, watches if DEBUG=True)",
        )

    def handle(self, *args, **options):
        # Handle interuption signals
        signal.signal(signal.SIGINT, self.handle_signal)
        # signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGTERM, signal.default_int_handler)
        # for simulating an exception in tests
        signal.signal(signal.SIGUSR1, self.handle_signal)
        # Handle termination (raises KeyboardInterrupt)
        # Custom signal to provoke an artifical exception, used for testing only

        # We store the PID in the environment, so it can be reused accross reloads
        pid = os.environ.setdefault("TOOSIMPLEQ_PID", f"{os.getpid()}")

        self.queues = options["queue"] or []
        self.excluded_queues = options["exclude_queue"] or []
        self.tick_duration = options["tick"]
        self.label = options["label"].replace(r"{pid}", pid)
        self.timeout = options["timeout"]
        self.exit_requested = False
        self.simulate_exception = False
        self.current_task_exec = None

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

        if options["reload"] == "always" or (
            settings.DEBUG and options["reload"] == "default"
        ):
            logger.info(f"Running with reloader !")
            autoreload.run_with_reloader(self.inner_run, options)
        else:
            self.inner_run(options)

    def inner_run(self, options):
        autoreload.raise_last_exception()

        # On startup, we report the worker went online
        self.worker_status, _ = WorkerStatus.objects.update_or_create(
            label=self.label,
            defaults={
                "started": timezone.now(),
                "stopped": None,
                "exit_code": None,
                "exit_log": None,
                "included_queues": self.queues,
                "excluded_queues": self.excluded_queues,
                "timeout": datetime.timedelta(seconds=self.timeout),
            },
        )

        try:
            last_run = timezone.now()
            while not self.exit_requested:
                if self.exit_requested:
                    break

                # for testing
                if self.simulate_exception:
                    raise FakeException()

                did_something = self.tick()

                if options["once"]:
                    logger.info("Exiting loop because --once was passed")
                    break

                if options["until_done"] and not did_something:
                    logger.info("Exiting loop because --until_done was passed")
                    break

                if not did_something:
                    logger.debug(f"Waiting for next tick...")
                    next_run = last_run + datetime.timedelta(seconds=self.tick_duration)
                    while not self.exit_requested and timezone.now() < next_run:
                        pass

                last_run = timezone.now()

            logger.critical(f"Gracefully finished")
            self.worker_status.exit_code = WorkerStatus.ExitCodes.STOPPED.value
            self.worker_status.exit_log = ""
        except (KeyboardInterrupt, SystemExit):
            logger.critical(f"Terminated by user request")
            self.worker_status.exit_code = WorkerStatus.ExitCodes.TERMINATED.value
            self.worker_status.exit_log = format_exc()
        except Exception as e:
            logger.critical(f"Crashed unhandled exception: {e}")
            self.worker_status.exit_code = WorkerStatus.ExitCodes.CRASHED.value
            self.worker_status.exit_log = format_exc()
        finally:
            self.worker_status.stopped = timezone.now()
            self.worker_status.save()

        if self.worker_status.exit_code:
            raise CommandError(returncode=self.worker_status.exit_code)

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
            self.current_task_exec = tasks_execs.select_for_update().first()
            if self.current_task_exec:
                logger.debug(f"{self.current_task_exec} is due !")
                self.current_task_exec.started = timezone.now()
                self.current_task_exec.state = TaskExec.States.PROCESSING
                self.current_task_exec.worker = self.worker_status
                self.current_task_exec.save()

        if self.current_task_exec:
            task = tasks_registry[self.current_task_exec.task_name]
            self.current_task = task
            did_something |= task.execute(self.current_task_exec)
            self.current_task_exec = None

        return did_something

    def handle_signal(self, sig, stackframe):
        # For testing, simulates a unexpected crash of the worker
        if sig == signal.SIGUSR1:
            self.simulate_exception = True
            return

        # A termination signal or a second interruption signal should force exit
        if sig == signal.SIGTERM or (sig == signal.SIGINT and self.exit_requested):
            logger.critical(f"User requested termination...")
            raise KeyboardInterrupt()

        # A first interruption signal is graceful exit
        if sig == signal.SIGINT:
            logger.critical(f"User requested graceful exit...")
            if self.current_task_exec is not None:
                logger.critical(f"Waiting for `{self.current_task_exec}` to finish...")
            self.exit_requested = True
