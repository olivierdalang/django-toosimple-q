import inspect
from datetime import timedelta

from django.core import management
from django.utils.timezone import now
from freezegun import freeze_time

from django_toosimple_q.decorators import register_task
from django_toosimple_q.models import TaskExec, WorkerStatus

from .base import TooSimpleQBackgroundTestCase, TooSimpleQRegularTestCase
from .concurrency.tasks import output_string_task


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
        self.assertCountEqual([w.state for w in ws], [S.OFFLINE])

        # A second call doesn't change it
        ws = WorkerStatus.objects.all()
        self.assertEqual(ws.count(), 1)
        self.assertCountEqual([w.label for w in ws], ["w1"])
        self.assertCountEqual([w.state for w in ws], [S.OFFLINE])

        # Another worker adds a new status
        management.call_command("worker", "--once", "--label", "w2")
        ws = WorkerStatus.objects.all()
        self.assertEqual(ws.count(), 2)
        self.assertCountEqual([w.label for w in ws], ["w1", "w2"])
        self.assertCountEqual([w.state for w in ws], [S.OFFLINE, S.OFFLINE])

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


class TestAutoreloadingWorker(TooSimpleQBackgroundTestCase):
    def _rewrite_function_in_place(self, old_text, new_text):
        path = inspect.getfile(output_string_task)
        with open(path, "r") as f:
            contents = f.read()
        contents = contents.replace(old_text, new_text)
        with open(path, "w") as f:
            f.write(contents)

    def test_schedules(self):
        # Start a worker
        self.workers_start_in_background(
            queue="tasks",
            count=1,
            tick=1,
            until_done=False,
            reload="always",
            verbosity=3,
        )

        # The reloader needs some time to initially start up and to reload. This
        # means we must wait a little otherwise it won't pick up changes.
        # FIXME: for some reason, this seems required only with sqlite
        RELOADER_WAIT = timedelta(seconds=10)

        # Running the task with delay
        output_string_task.queue(due=now() + RELOADER_WAIT)
        self.wait_for_tasks()

        # Hot-changing the file, should reload the worker, and from now yield other results
        self._rewrite_function_in_place("***OUTPUT_A***", "***OUTPUT_B***")

        # Running the task with delay
        output_string_task.queue(due=now() + RELOADER_WAIT)
        self.wait_for_tasks()

        # Hot-changing the file, should reload the worker, and from now yield other results
        self._rewrite_function_in_place("***OUTPUT_B***", "***OUTPUT_A***")

        # Running the task with delay
        output_string_task.queue(due=now() + RELOADER_WAIT)
        self.wait_for_tasks()

        # Stop the worker
        self.workers_gracefully_stop()

        # Get the output
        output = self.workers_wait_for_success()

        self.assertTrue(
            "tasks.py changed, reloading." in output,
            f"Output does not contain any mention of file change.\n"
            f"If getting inconsistent results, consider increasing RELOADER_WAIT.\nFull output:\n{output}",
        )
        self.assertEqual(
            [
                t.result
                for t in TaskExec.objects.order_by("created", "started", "finished")
            ],
            ["***OUTPUT_A***", "***OUTPUT_B***", "***OUTPUT_A***"],
            f"If getting inconsistent results, consider increasing RELOADER_WAIT.\nFull output:\n{output}",
        )
