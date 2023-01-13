import datetime
import logging
import os
import signal
import sys
from traceback import format_exc

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError, transaction
from django.db.models import Case, Value, When
from django.utils import autoreload
from django.utils.timezone import now

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

        # TODO: replace by simple-parsing
        self.queues = options["queue"] or []
        self.excluded_queues = options["exclude_queue"] or []
        self.tick_duration = options["tick"]
        self.timeout = options["timeout"]
        self.once = options["once"]
        self.until_done = options["until_done"]

        self.reloader_active = options["reload"] == "always" or (
            settings.DEBUG and options["reload"] == "default"
        )

        self.label = options["label"].replace(r"{pid}", pid)

        self.exit_requested = False
        self.simulate_exception = False
        self.cur_task_exec = None

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

        if self.reloader_active:
            logger.info(f"Running with reloader !")
            autoreload.run_with_reloader(self.inner_run)
        else:
            self.inner_run()

    def inner_run(self):
        autoreload.raise_last_exception()

        # On startup, we report the worker went online
        logger.debug(f"Get or create worker instance")
        self.worker_status, _ = WorkerStatus.objects.update_or_create(
            label=self.label,
            defaults={
                "started": now(),
                "stopped": None,
                "exit_code": None,
                "exit_log": None,
                "included_queues": self.queues,
                "excluded_queues": self.excluded_queues,
                "timeout": datetime.timedelta(seconds=self.timeout),
            },
        )

        exc = None

        try:
            # Run the loop
            while self.do_loop():
                pass

            self.worker_status.exit_code = WorkerStatus.ExitCodes.STOPPED.value
            self.worker_status.exit_log = ""

        except (KeyboardInterrupt, SystemExit) as e:
            exc = e
            logger.critical(f"Terminated by user request")
            if self.cur_task_exec:
                logger.critical(f"{self.cur_task_exec} got terminated !")
                self.cur_task_exec.state = TaskExec.States.INTERRUPTED
                self.cur_task_exec.save()
                self.cur_task_exec.create_replacement(is_retry=False)
                self.cur_task_exec = None
            self.worker_status.exit_code = WorkerStatus.ExitCodes.TERMINATED.value
            self.worker_status.exit_log = format_exc()

        except Exception as e:
            exc = e
            logger.critical(f"Crashed unhandled exception: {e}")
            self.worker_status.exit_code = WorkerStatus.ExitCodes.CRASHED.value
            self.worker_status.exit_log = format_exc()

        finally:
            self.worker_status.stopped = now()
            self.worker_status.save()

        if self.worker_status.exit_code:
            raise CommandError(returncode=self.worker_status.exit_code) from exc

        if self.reloader_active:
            sys.exit(0)

    def do_loop(self) -> bool:
        """Runs one tick, returns True if it should continue looping"""

        logger.debug(f"Tick !")

        last_run = now()

        did_something = False

        logger.debug(f"1. Update status...")
        self.worker_status.last_tick = now()
        self.worker_status.save()

        logger.debug(f"2. Disabling orphaned schedules")
        with transaction.atomic():
            if (
                ScheduleExec.objects.exclude(state=ScheduleExec.States.INVALID)
                .exclude(name__in=schedules_registry.keys())
                .update(state=ScheduleExec.States.INVALID)
            ):
                logger.warning(f"Found invalid schedules")

        logger.debug(f"3. Disabling orphaned tasks")
        with transaction.atomic():
            if (
                TaskExec.objects.exclude(state=TaskExec.States.INVALID)
                .exclude(task_name__in=tasks_registry.keys())
                .update(state=TaskExec.States.INVALID)
            ):
                logger.warning(f"Found invalid tasks")

        logger.debug(f"4. Create missing schedules")
        existing_schedules_names = ScheduleExec.objects.values_list("name", flat=True)
        for schedule in self._relevant_schedules:
            # Create the schedule exec if it does not exist
            if schedule.name in existing_schedules_names:
                continue
            try:
                last_due = None if schedule.run_on_creation else now()
                ScheduleExec.objects.create(name=schedule.name, last_due=last_due)
                logger.debug(f"Created schedule {schedule.name}")
            except IntegrityError:
                # This could happen with concurrent workers, and can be ignored
                logger.debug(
                    f"Schedule {schedule.name} already exists, probably created concurrently"
                )

        logger.debug(f"5. Execute schedules")
        with transaction.atomic():
            for schedule_exec in self._build_schedules_list_qs():
                did_something |= schedule_exec.execute()

        logger.debug(f"6. Waking up tasks")
        TaskExec.objects.filter(state=TaskExec.States.SLEEPING).filter(
            due__lte=now()
        ).update(state=TaskExec.States.QUEUED)

        logger.debug(f"7. Locking task")
        with transaction.atomic():
            self.cur_task_exec = self._build_due_tasks_qs().first()
            if self.cur_task_exec:
                logger.debug(f"{self.cur_task_exec} is due !")
                self.cur_task_exec.started = now()
                self.cur_task_exec.state = TaskExec.States.PROCESSING
                self.cur_task_exec.worker = self.worker_status
                self.cur_task_exec.save()

        logger.debug(f"8. Running task")
        if self.cur_task_exec:
            logger.debug(f"Executing : {self.cur_task_exec}")
            did_something = True
            self.cur_task_exec.execute()
            self.cur_task_exec = None

        if self.once:
            logger.info("Exiting loop because --once was passed")
            return False

        if self.until_done and not did_something:
            logger.info("Exiting loop because --until_done was passed")
            return False

        if self.exit_requested:
            logger.critical("Exiting gracefully on user request")
            return False

        if self.simulate_exception:
            # for testing
            raise FakeException()

        if not did_something:
            logger.debug(f"Waiting for next tick...")
            next_run = last_run + datetime.timedelta(seconds=self.tick_duration)
            while not self.exit_requested and now() < next_run:
                pass

        return True

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
            if self.cur_task_exec is not None:
                logger.critical(f"Waiting for `{self.cur_task_exec}` to finish...")
            self.exit_requested = True

    @property
    def _relevant_schedules(self):
        """Get a list of schedules for this worker"""
        return schedules_registry.for_queue(self.queues, self.excluded_queues)

    def _build_schedules_list_qs(self):
        """The queryset to select the list of schedules for update"""

        return ScheduleExec.objects.filter(
            name__in=[s.name for s in self._relevant_schedules]
        ).select_for_update(skip_locked=True)

    @property
    def _relevant_tasks(self):
        """Get a list of tasks for this worker"""
        return tasks_registry.for_queue(self.queues, self.excluded_queues)

    def _build_due_tasks_qs(self):
        """The queryset to select the task due by this worker for update"""

        # Build a order_by clause using the task priorities
        whens = [
            When(task_name=t.name, then=Value(-t.priority))
            for t in self._relevant_tasks
        ]
        order_by_priority = Case(*whens, default=Value(0))

        # Build the queryset
        return (
            TaskExec.objects.filter(state=TaskExec.States.QUEUED)
            .filter(task_name__in=[t.name for t in self._relevant_tasks])
            .order_by(order_by_priority, "due", "created")
            .select_for_update(skip_locked=True)
        )
