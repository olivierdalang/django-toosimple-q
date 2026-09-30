import time
from datetime import datetime

from django.core import management
from django.test import override_settings
from freezegun import freeze_time

from django_toosimple_q.decorators import register_task, schedule_task
from django_toosimple_q.models import TaskExec
from django_toosimple_q.registry import schedules_registry, tasks_registry

from .base import TooSimpleQBackgroundTestCase, TooSimpleQRegularTestCase


class TestRegressionBackground(TooSimpleQBackgroundTestCase):
    def test_regr_schedule_short(self):
        # Regression test for an issue where a schedule with smaller periods was not always processed

        # A worker that ticks every second should trigger a schedule due every second
        self.start_worker_in_background(
            queue="regr_schedule_short", tick=1, until_done=False, verbosity=3
        )
        time.sleep(20)

        # It should do almost 20 tasks
        self.assertGreaterEqual(TaskExec.objects.all().count(), 18)


class TestRegressionRegular(TooSimpleQRegularTestCase):
    def test_deleting_schedule(self):
        # Regression test for an issue where deleting a schedule in code would crash the admin view

        @schedule_task(cron="0 12 * * *", datetime_kwarg="scheduled_on")
        @register_task(name="normal")
        def a(scheduled_on):
            return f"{scheduled_on:%Y-%m-%d %H:%M}"

        management.call_command("worker", "--until_done")

        schedules_registry.clear()
        tasks_registry.clear()

        # the admin view still works even for deleted schedules
        response = self.client.get("/admin/toosimpleq/scheduleexec/")
        self.assertEqual(response.status_code, 200)

    def test_different_schedule_and_task(self):
        # Regression test for an issue where schedule with a different name than the task would fail

        @schedule_task(cron="0 12 * * *", name="name_a", run_on_creation=True)
        @register_task(name="name_b")
        def a():
            return True

        management.call_command("worker", "--until_done")

    @freeze_time(datetime(2020, 1, 1, 12, 0))
    @override_settings(TIME_ZONE="Europe/Zurich")
    def test_schedules_ran_twice_with_timezone_and_on_creation(self):
        # Regression test for an issue with cron range calculation which wasn't honoring timezone, so schedules would run twice
        @schedule_task(cron="0 2 * * *", name="name_a", run_on_creation=True)
        @register_task(name="name_a")
        def a():
            return True

        management.call_command("worker", "--until_done")
        taskexecs = TaskExec.objects.filter()
        self.assertEqual(taskexecs.count(), 1)
