import os
import signal
import subprocess

from django.contrib.auth.models import User
from django.core import mail
from django.db.models import Count
from django.test import Client, TestCase, TransactionTestCase

from django_toosimple_q.models import ScheduleExec, TaskExec
from django_toosimple_q.registry import schedules_registry, tasks_registry

from ..logging import logger
from .utils import is_postgres, prepare_toxiproxy


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
    - Starts a toxiproy to simulate latency for the background workers
    - Adds some helpers methods to manage the workers

    See TooSimpleQTestCaseMixin.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if is_postgres():
            prepare_toxiproxy()

    def workers_start_in_background(self, queue=None, count=1):
        """Starts N workers in the background on the specified queue."""

        environ = {
            **os.environ,
            "DJANGO_SETTINGS_MODULE": "django_toosimple_q.tests.concurrency.settings",
        }
        command = ["python", "manage.py", "worker", "--until_done", "--skip-checks"]
        if queue:
            command.extend(["--queue", queue])

        self.processes = []
        for _ in range(count):
            self.processes.append(
                subprocess.Popen(
                    command,
                    encoding="utf-8",
                    env=environ,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            )

    def workers_wait_for_success(self):
        """Waits for all processes to finish, and assert they succeeded."""

        self.stdouts, self.stderrs = [], []
        for process in self.processes:
            stdout, stderr = process.communicate(timeout=15)
            self.stdouts.append(stdout)
            self.stderrs.append(stderr)
            process.wait()
        self.workers_assert_no_error()

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

    def workers_assert_no_error(self):
        error_count = len([p for p in self.processes if p.returncode != 0])
        total_count = len(self.processes)
        if error_count:
            # show last output
            def indent(txt):
                return "\n".join([" >  " + l for l in txt.splitlines()])

            logger.exception("\n" + indent(self.stdouts[-1]))
            logger.exception("\n" + indent(self.stderrs[-1]))
            raise AssertionError(f"{error_count} out of {total_count} workers errored!")
