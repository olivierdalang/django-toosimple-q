import os
import signal
import subprocess
import time
from typing import List

from django.contrib.auth.models import User
from django.core import mail
from django.db.models import Count
from django.test import Client, TestCase, TransactionTestCase

from django_toosimple_q.models import ScheduleExec, TaskExec
from django_toosimple_q.registry import schedules_registry, tasks_registry

from ..logging import logger


class TooSimpleQTestCaseMixin:
    """
    Base TestCase for TooSimpleQ.

    - Clears the schedules and task registries (reverting the autodiscovery)
    - Creates a superuser
    - Clears the mailbox
    - Adds assertQueue and assertTask helpers

    Use this if you want to keep autoloaded task (from contrib.mail) from polluting the tests.
    """

    def setUp(self):
        # Clean the registry
        schedules_registry.clear()
        tasks_registry.clear()

        # Create a superuser
        user = User.objects.create_superuser("admin", "test@example.com", "pass")
        self.client = Client()
        self.client.force_login(user)

        # Clear the mailbox
        mail.outbox.clear()

    def assertQueue(
        self, expected_count, task_name=None, state=None, replaced=None, due=None
    ):
        tasks = TaskExec.objects.all()
        if task_name:
            tasks = tasks.filter(task_name=task_name)
        if state:
            tasks = tasks.filter(state=state)
        if replaced is not None:
            tasks = tasks.filter(replaced_by__isnull=not replaced)
        if due is not None:
            tasks = tasks.filter(due=due)
        actual_count = tasks.count()
        if actual_count != expected_count:
            vals = (
                TaskExec.objects.values("task_name", "state")
                .annotate(count=Count("*"))
                .order_by("task_name", "state")
            )
            debug = "\n".join(
                f"{v['task_name']}/{v['state']} : {v['count']}" for v in vals
            )
            raise AssertionError(
                f"Expected {expected_count} tasks, got {actual_count} tasks.\n{debug}"
            )

    def assertResults(self, expected=[], task_name=None):
        tasks_execs = TaskExec.objects.order_by("created", "result")
        if task_name:
            tasks_execs = tasks_execs.filter(task_name=task_name)
        results = list(tasks_execs.values_list("result", flat=True))

        self.assertEqual(results, expected)

    def assertTask(self, task, expected_state):
        actual_state = TaskExec.objects.get(pk=task.pk).state
        if actual_state != expected_state:
            raise AssertionError(
                f"Expected {expected_state}, got {actual_state} [{task}]"
            )

    def assertSchedule(self, name, expected_state):
        try:
            state = ScheduleExec.objects.get(name=name).state
        except ScheduleExec.DoesNotExist:
            state = None

        if state != expected_state:
            raise AssertionError(f"Expected {expected_state}, got {state} [{name}]")


class TooSimpleQRegularTestCase(TooSimpleQTestCaseMixin, TestCase):
    """
    Base TestCase for TooSimpleQ.

    See TooSimpleQTestCaseMixin
    """


class TooSimpleQBackgroundTestCase(TransactionTestCase):
    """
    Base TransactionTestCase for TooSimpleQ.

    - Ensures the database is accessible from background workers (transactiontestcase do no wrap tests in transactions)
    - Adds some helpers methods to manage the workers

    See TooSimpleQTestCaseMixin.
    """

    postgres_lag_for_background_worker = False

    def setUp(self):
        super().setUp()
        self.processes: List[subprocess.Popen] = []

    def tearDown(self):
        super().tearDown()

        self.workers_gracefully_stop()
        self.workers_kill()

    def start_worker_in_background(
        self,
        queue=None,
        tick=None,
        until_done=True,
        once=False,
        skip_checks=True,
        reload="never",
        verbosity=None,
        label=None,
        timeout=None,
    ):
        """Starts N workers in the background on the specified queue."""

        if self.postgres_lag_for_background_worker:
            settings = "django_toosimple_q.tests.settings_bg_lag"
        else:
            settings = "django_toosimple_q.tests.settings_bg"

        command = ["python", "manage.py", "worker"]

        if label:
            command.extend(["--label", str(label)])
        if tick:
            command.extend(["--tick", str(tick)])
        if queue:
            command.extend(["--queue", str(queue)])
        if until_done:
            command.extend(["--until_done"])
        if once:
            command.extend(["--once"])
        if skip_checks:
            command.extend(["--skip-checks"])
        if reload:
            command.extend(["--reload", str(reload)])
        if verbosity:
            command.extend(["--verbosity", str(verbosity)])
        if timeout:
            command.extend(["--timeout", str(timeout)])

        logger.debug(f"Starting workers: {' '.join(command)}")
        self.processes.append(
            subprocess.Popen(
                command,
                encoding="utf-8",
                env={**os.environ, "DJANGO_SETTINGS_MODULE": settings},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        )

    def wait_for_qs(self, queryset, exists=True, timeout=15):
        """Waits until the queryset exists (or does not exist)"""
        start_time = time.time()
        while queryset.exists() == (not exists):
            if (time.time() - start_time) > timeout:
                raise AssertionError(
                    f"Expected queryset was not present after {timeout} seconds"
                    if exists
                    else f"Unexpected queryset was still present after {timeout} seconds"
                )

    def wait_for_tasks(self, timeout=15):
        """Waits untill all tasks are marked as done in the database"""
        return self.wait_for_qs(
            TaskExec.objects.filter(state__in=TaskExec.States.todo()),
            exists=False,
            timeout=timeout,
        )

    def workers_get_stdout(self):
        """Stops the workers if needed and returns the stdout of the last worker, or raises an exception on error.

        Can be used to check output or to assert success"""

        outputs = []
        for process in self.processes:
            try:
                stdout, stderr = process.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
            outputs.append((process.returncode, stdout, stderr))

        # Outputs that errored
        error_outputs = [o for o in outputs if o[0] != 0]

        # Last output is last one
        last_output = error_outputs[-1] if error_outputs else outputs[-1]

        last_retcod, last_stdout, last_stderr = last_output

        # Raise exception if error
        if last_retcod != 0:
            all_retcodes = ", ".join([f"{r[0]}" for r in outputs])
            logger.warn(f"Some workers errored. All return codes: {all_retcodes}\n")
            logger.warn(f"Last error retcod:\n{last_retcod}\n")
            logger.warn(f"Last error stdout:\n{last_stdout}\n")
            logger.warn(f"Last error stderr:\n{last_stderr}")
            raise AssertionError(f"Some workers errored.")

        return last_stdout

    def workers_gracefully_stop(self):
        """Gracefully stops all workers (note that you must still wait for them to finish using wait_for_success)."""

        for process in self.processes:
            if os.name == "nt":
                # FIXME: This is buggy. When running, test passes, but then the test stops, and further
                # tests are not run. Not sure if sending CTRL_C to the child process also affects the current
                # process for some reason ?
                process.send_signal(signal.CTRL_C_EVENT)
            else:
                process.send_signal(signal.SIGTERM)

    def workers_kill(self):
        """Forces kill all processes (use this for cleanup)"""

        open_processes = [p for p in self.processes if p.poll() is None]
        if not open_processes:
            return

        logger.warn(f"Killing {len(open_processes)} dangling worker processes...")
        for process in open_processes:
            process.kill()
