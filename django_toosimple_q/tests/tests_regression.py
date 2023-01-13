import time

from django_toosimple_q.models import TaskExec

from .base import TooSimpleQBackgroundTestCase


class TestRegression(TooSimpleQBackgroundTestCase):
    def test_regr_schedule_short(self):
        # Regression test for an issue where a schedule with smaller periods was not always processed

        # A worker that ticks every second should trigger a schedule due every second
        self.start_worker_in_background(
            queue="regr_schedule_short", tick=1, until_done=False, verbosity=3
        )
        time.sleep(20)

        # It should do almost 20 tasks
        self.assertGreaterEqual(TaskExec.objects.all().count(), 18)
