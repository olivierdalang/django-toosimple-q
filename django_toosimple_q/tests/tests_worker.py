from django.core import management
from freezegun import freeze_time

from django_toosimple_q.decorators import register_task
from django_toosimple_q.models import WorkerStatus

from .base import TooSimpleQRegularTestCase


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
