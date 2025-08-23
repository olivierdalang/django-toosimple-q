import os
import signal
import time
import unittest

from django.core import management
from freezegun import freeze_time

from django_toosimple_q.decorators import register_task
from django_toosimple_q.models import TaskExec, WorkerStatus

from ..models import WorkerStatus
from .base import TooSimpleQBackgroundTestCase, TooSimpleQRegularTestCase
from .concurrency.tasks import sleep_task


class TestWorker(TooSimpleQRegularTestCase):
    @freeze_time("2020-01-01", as_kwarg="frozen_datetime")
    def test_worker(self, frozen_datetime):
        """Checking that worker status are correctly created"""

        # Syntaxic sugar
        S = WorkerStatus.States

        # Workers status is correctly created
        management.call_command("worker", "--once", "--label", "w1")
        ws = WorkerStatus.objects.all()
        self.assertEqual(ws.count(), 1)
        self.assertCountEqual([w.label for w in ws], ["w1"])
        self.assertCountEqual([w.state for w in ws], [S.STOPPED])

        # A second call doesn't change it
        ws = WorkerStatus.objects.all()
        self.assertEqual(ws.count(), 1)
        self.assertCountEqual([w.label for w in ws], ["w1"])
        self.assertCountEqual([w.state for w in ws], [S.STOPPED])

        # Another worker adds a new status
        management.call_command("worker", "--once", "--label", "w2")
        ws = WorkerStatus.objects.all()
        self.assertEqual(ws.count(), 2)
        self.assertCountEqual([w.label for w in ws], ["w1", "w2"])
        self.assertCountEqual([w.state for w in ws], [S.STOPPED, S.STOPPED])

        # Worker are properly populating attached to task
        @register_task(name="a")
        def a():
            return True

        t = a.queue()
        self.assertEqual(t.worker, None)
        management.call_command("worker", "--once", "--label", "w2")
        t.refresh_from_db()
        self.assertEqual(t.worker.label, "w2")

    # TODO: test for worker timeout status
    # TODO: test for no label/pid clashes with multiple workers


class TestWorkerExit(TooSimpleQBackgroundTestCase):
    """This tests all types of worker termination (kill, terminate, quit, crash) and their effect on tasks and output states"""

    def _start_worker_with_task(self, duration=10):
        """Helper to create a worker that picked up a 10s task"""

        # Add a task that takes some time
        sleep_task.queue(duration=duration)

        # Start a worker
        self.start_worker_in_background(
            queue="tasks", tick=1, until_done=False, verbosity=3, timeout=3
        )

        # Wait for the task to be picked up by the worker
        try:
            self.wait_for_qs(TaskExec.objects.filter(state=TaskExec.States.PROCESSING))
        except AssertionError:
            # Show the error
            self.workers_get_stdout()

        # Keep id of the first task for further reference
        self.__first_task_pk = TaskExec.objects.first().pk

        # Let the worker actually start processing the task
        # FIXME: this should not be needed
        time.sleep(1)

    @property
    def workerstatus(self):
        """The workerstatus object corresponding to the worker"""
        return WorkerStatus.objects.first()

    @property
    def taskexec(self):
        """The long running task that should have been picked up by the worker"""
        return TaskExec.objects.get(pk=self.__first_task_pk)

    @property
    def process(self):
        """The subprocess"""
        return self.processes[0]

    def test_kill(self):
        self._start_worker_with_task()

        # Hard kill, the worker can't cleanly quit
        self.process.kill()

        # -9 is exit code for SIGKILL
        exit_code = self.process.wait(timeout=5)
        self.assertEqual(exit_code, -9)

        # Initially the worker still looks online
        self.assertEqual(self.workerstatus.state, WorkerStatus.States.ONLINE)

        # The task also will look like it's still processing
        self.assertEqual(self.taskexec.state, TaskExec.States.PROCESSING)

        # After a while though, it should timeout
        time.sleep(5)
        self.assertEqual(self.workerstatus.state, WorkerStatus.States.TIMEDOUT)
        self.assertIsNone(self.workerstatus.exit_log)

        # Same for the task
        # FIXME: we have no timeout status for tasks, so it stays processing indefinitely. We should
        # add a timeout status and assign it
        self.assertEqual(self.taskexec.state, TaskExec.States.PROCESSING)

    def test_terminate(self):
        self._start_worker_with_task()

        # Soft kill, the worker should interrupt the task and quit as soon as possible
        self.process.send_signal(signal.SIGTERM)

        # We should have our custom exit code
        exit_code = self.process.wait(timeout=5)
        self.assertEqual(exit_code, WorkerStatus.ExitCodes.TERMINATED.value)

        # The worker should correctly set its state
        self.assertEqual(self.workerstatus.state, WorkerStatus.States.TERMINATED)
        self.assertIn("KeyboardInterrupt", self.workerstatus.exit_log)

        # The task should be noted as interrupted, and replaced by another task
        self.assertEqual(self.taskexec.state, TaskExec.States.INTERRUPTED)
        self.assertIsNone(self.taskexec.result)
        self.assertIsNotNone(self.taskexec.replaced_by)
        self.assertEqual(self.taskexec.replaced_by.state, TaskExec.States.SLEEPING)

    @unittest.skipIf(
        not hasattr(signal, "SIGUSR1"), "USR1 signal not available on this platform"
    )
    def test_worker_crash(self):
        self._start_worker_with_task()

        # Simluate a crash by deleting the workerexec instance
        self.process.send_signal(signal.SIGUSR1)

        # The exit code should be 0, it's a graceful quit
        exit_code = self.process.wait(timeout=15)
        self.assertEqual(exit_code, WorkerStatus.ExitCodes.CRASHED.value)

        # The worker should correctly set its state
        self.assertEqual(self.workerstatus.state, WorkerStatus.States.CRASHED)
        self.assertIn("FakeException", self.workerstatus.exit_log)

        # The failure is not linked to the task

    @unittest.skipIf(
        os.name == "nt", "didn't find a way to gracefully stop subprocess on windows"
    )
    def test_quit(self):
        self._start_worker_with_task()

        # Regular quit, the process should let the task finish, then exit
        self.process.send_signal(signal.SIGINT)

        # The exit code should be 0, it's a graceful quit
        exit_code = self.process.wait(timeout=15)
        self.assertEqual(exit_code, WorkerStatus.ExitCodes.STOPPED.value)

        # The worker should correctly set its state
        self.assertEqual(self.workerstatus.state, WorkerStatus.States.STOPPED)
        self.assertEqual("", self.workerstatus.exit_log)

        # The task should have been left enough time to finish
        self.assertEqual(self.taskexec.state, TaskExec.States.SUCCEEDED)
        self.assertTrue(self.taskexec.result)
