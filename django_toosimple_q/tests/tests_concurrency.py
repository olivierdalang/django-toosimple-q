import os
import time
import unittest

from django.contrib.auth.models import User

from django_toosimple_q.models import ScheduleExec, TaskExec

from .base import TooSimpleQBackgroundTestCase
from .concurrency.tasks import create_user, sleep_task

COUNT = 32


# FIXME: not sure if we really can't have this working on SQLITE ?
@unittest.skipIf(
    os.environ.get("TOOSIMPLEQ_TEST_DB") != "postgres", "requires postgres backend"
)
class ConcurrencyTest(TooSimpleQBackgroundTestCase):
    """This runs some concurrency tests. It sets up a database with simulated lag to
    increase race conditions likelyhood, thus requires a running docker daemon."""

    def test_schedules(self):

        # Ensure no duplicate schedules were created
        self.assertEqual(ScheduleExec.objects.count(), 0)
        self.workers_start_in_background(queue="schedules", count=COUNT)
        self.workers_wait_for_success()
        self.assertEqual(ScheduleExec.objects.count(), 1)

    def test_tasks(self):

        create_user.queue()

        # Ensure the task really was just executed once
        self.assertEqual(User.objects.count(), 0)
        self.workers_start_in_background(queue="tasks", count=COUNT)
        self.workers_wait_for_success()
        self.assertEqual(User.objects.count(), 1)

    def test_task_processing_state(self):

        t = sleep_task.queue(duration=10)

        # Check that the task correctly queued
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.QUEUED)

        # Start the task in a background process
        self.workers_start_in_background(queue="tasks", count=1)

        # Check that it is now processing
        time.sleep(5)
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.PROCESSING)

        # Wait for the background process to finish
        self.workers_wait_for_success()

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
        self.workers_start_in_background(queue="tasks", count=1)

        # Check that it is now processing
        time.sleep(5)
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.PROCESSING)

        # Gracefully stop the background process
        self.workers_gracefully_stop()

        # Wait for the background process to finish
        self.workers_wait_for_success()

        # Check that the state is correctly set to interrupted and that a replacing task was added
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.INTERRUPTED)
        self.assertEqual(t.replaced_by.state, TaskExec.States.SLEEPING)
