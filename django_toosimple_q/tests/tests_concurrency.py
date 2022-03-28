import os
import signal
import time
import unittest

from django.contrib.auth.models import User
from django.test import TransactionTestCase
from joblib import Parallel, delayed

from django_toosimple_q.models import ScheduleExec, TaskExec

from ..logging import logger
from .concurrency.tasks import create_user, sleep_task
from .concurrency.utils import prepare_toxiproxy, sys_call

COUNT = 32


@unittest.skipIf(
    os.environ.get("TOOSIMPLEQ_TEST_DB") != "postgres", "requires postgres backend"
)
class ConcurrencyTest(TransactionTestCase):
    """This runs some concurrency tests. It sets up a database with simulated lag to
    increase race conditions likelyhood, thus requires a running docker daemon."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        prepare_toxiproxy()

    def make_command(self, queue=None):
        command = ["python", "manage.py", "worker", "--until_done", "--skip-checks"]
        if queue:
            command.extend(["--queue", queue])
        return command

    def run_workers_batch(self, command):
        outputs = Parallel(n_jobs=COUNT, backend="threading")(
            delayed(sys_call)(command) for i in range(COUNT)
        )

        # Ensure no worker failed
        errored_ouputs = [o for o in outputs if o.returncode != 0]
        if errored_ouputs:
            logger.exception(f"Last output:")

            def indent(txt):
                return "\n".join([" >  " + l for l in txt.splitlines()])

            logger.exception("\n" + indent(errored_ouputs[-1].stdout))
            logger.exception("\n" + indent(errored_ouputs[-1].stderr))
            raise AssertionError(
                f"{len(errored_ouputs)} out of {len(outputs)} workers errored!"
            )

    def test_schedules(self):

        # Ensure no duplicate schedules were created
        self.assertEqual(ScheduleExec.objects.count(), 0)
        self.run_workers_batch(self.make_command(queue="schedules"))
        self.assertEqual(ScheduleExec.objects.count(), 1)

    def test_tasks(self):

        create_user.queue()

        # Ensure the task really was just executed once
        self.assertEqual(User.objects.count(), 0)
        self.run_workers_batch(self.make_command(queue="tasks"))
        self.assertEqual(User.objects.count(), 1)

    def test_task_processing_state(self):

        t = sleep_task.queue(duration=10)

        # Check that the task correctly queued
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.QUEUED)

        # Start the task in a background process
        popen = sys_call(self.make_command(queue="tasks"), sync=False)

        # Check that it is now processing
        time.sleep(5)
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.PROCESSING)

        # Wait for the background process to finish
        popen.communicate(timeout=15)

        # Check that it correctly succeeds
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.SUCCEEDED)

    def test_task_graceful_stop(self):
        """Ensure that on graceful stop, running tasks status is set to interrupted and a replacement task is created"""

        t = sleep_task.queue(duration=10)

        # Check that the task correctly queued
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.QUEUED)

        # Start the task in a background process
        popen = sys_call(self.make_command(queue="tasks"), sync=False)

        # Check that it is now processing
        time.sleep(5)
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.PROCESSING)

        # Gracefully stop the background process to finish
        if os.name == "nt":
            popen.send_signal(signal.CTRL_C_EVENT)
        else:
            popen.send_signal(signal.SIGTERM)
        popen.communicate(timeout=15)

        # Check that the state is correctly set to interrupted and that a replacing task was added
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.INTERRUPTED)
        self.assertEqual(t.replaced_by.state, TaskExec.States.SLEEPING)
