import os
import time
import unittest

from django.contrib.auth.models import User

from django_toosimple_q.models import TaskExec, WorkerStatus

from .base import TooSimpleQBackgroundTestCase
from .concurrency.tasks import create_user, sleep_task
from .utils import is_postgres

COUNT = 32


# FIXME: not sure if we really can't have this working on SQLITE ?
@unittest.skipIf(not is_postgres(), "requires postgres backend")
class ConcurrencyTest(TooSimpleQBackgroundTestCase):
    """This runs some concurrency tests. It sets up a database with simulated lag to
    increase race conditions likelyhood, thus requires a running docker daemon."""

    postgres_lag_for_background_worker = True

    def test_schedules(self):
        # We create COUNT workers with different labels
        for i in range(COUNT):
            self.start_worker_in_background(
                queue="schedules",
                label=f"w-{i}",
                verbosity=3,
                once=True,
                until_done=False,
            )
        self.workers_get_stdout()

        # Ensure they were all created
        self.assertEqual(WorkerStatus.objects.count(), COUNT)

        # The schedule should have run just once and thus the task only queued once despite run_on_creation
        self.assertEqual(TaskExec.objects.count(), 1)

    def test_tasks(self):
        # Create a task
        create_user.queue()

        self.assertEqual(User.objects.count(), 0)

        # We create COUNT workers with different labels
        for i in range(COUNT):
            self.start_worker_in_background(
                queue="tasks", label=f"w-{i}", verbosity=3, once=True, until_done=False
            )
        self.workers_get_stdout()

        # Ensure they were all created
        self.assertEqual(WorkerStatus.objects.count(), COUNT)

        # The task shouldn't have run concurrently and thus have run only once
        self.assertEqual(User.objects.count(), 1)

    def test_task_processing_state(self):
        t = sleep_task.queue(duration=10)

        # Check that the task correctly queued
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.QUEUED)

        # Start the task in a background process
        self.start_worker_in_background(queue="tasks")

        # Check that it is now processing
        time.sleep(5)
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.PROCESSING)

        # Wait for the background process to finish
        self.workers_get_stdout()

        # Check that it correctly succeeds
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.SUCCEEDED)

    @unittest.skipIf(
        os.name == "nt", "didn't find a way to gracefully stop subprocess on windows"
    )
    def test_task_graceful_stop(self):
        """Ensure that on graceful stop, running tasks status is set to interrupted and a replacement task is created"""

        t = sleep_task.queue(duration=10)

        # Check that the task correctly queued
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.QUEUED)

        # Start the task in a background process
        self.start_worker_in_background(queue="tasks")

        # Check that it is now processing
        time.sleep(5)
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.PROCESSING)

        # Gracefully stop the background process
        self.workers_gracefully_stop()

        # Wait for the background process to finish
        self.processes[0].wait(timeout=5)

        # Check that the state is correctly set to interrupted and that a replacing task was added
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.INTERRUPTED)
        self.assertEqual(t.replaced_by.state, TaskExec.States.SLEEPING)
