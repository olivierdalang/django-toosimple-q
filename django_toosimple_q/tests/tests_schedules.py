from django.core import management
from freezegun import freeze_time

from django_toosimple_q.decorators import register_task, schedule_task
from django_toosimple_q.models import ScheduleExec, TaskExec
from django_toosimple_q.registry import schedules_registry

from .base import TooSimpleQRegularTestCase


class TestSchedules(TooSimpleQRegularTestCase):
    @freeze_time("2020-01-01", as_kwarg="frozen_datetime")
    def test_schedule(self, frozen_datetime):
        """Testing schedules"""

        @schedule_task(cron="0 12 * * *", datetime_kwarg="scheduled_on")
        @register_task(name="normal")
        def a(scheduled_on):
            return f"{scheduled_on:%Y-%m-%d %H:%M}"

        @schedule_task(
            cron="0 12 * * *", run_on_creation=True, datetime_kwarg="scheduled_on"
        )
        @register_task(name="autostart")
        def b(scheduled_on):
            return f"{scheduled_on:%Y-%m-%d %H:%M}"

        @schedule_task(cron="0 12 * * *", catch_up=True, datetime_kwarg="scheduled_on")
        @register_task(name="catchup")
        def c(scheduled_on):
            return f"{scheduled_on:%Y-%m-%d %H:%M}"

        @schedule_task(
            cron="0 12 * * *",
            run_on_creation=True,
            catch_up=True,
            datetime_kwarg="scheduled_on",
        )
        @register_task(name="autostartcatchup")
        def d(scheduled_on):
            return f"{scheduled_on:%Y-%m-%d %H:%M}"

        self.assertEquals(len(schedules_registry), 4)
        self.assertEquals(ScheduleExec.objects.count(), 0)
        self.assertQueue(0)

        management.call_command("worker", "--until_done")

        # first run, only tasks with run_on_creation=True should run as no time passed
        self.assertQueue(0, task_name="normal")
        self.assertQueue(1, task_name="autostart")
        self.assertQueue(0, task_name="catchup")
        self.assertQueue(1, task_name="autostartcatchup")
        self.assertQueue(2)

        management.call_command("worker", "--until_done")

        # second run, no time passed so no change
        self.assertQueue(0, task_name="normal")
        self.assertQueue(1, task_name="autostart")
        self.assertQueue(0, task_name="catchup")
        self.assertQueue(1, task_name="autostartcatchup")
        self.assertQueue(2)

        frozen_datetime.move_to("2020-01-02")
        management.call_command("worker", "--until_done")

        # one day passed, all tasks should have run once
        self.assertQueue(1, task_name="normal")
        self.assertQueue(2, task_name="autostart")
        self.assertQueue(1, task_name="catchup")
        self.assertQueue(2, task_name="autostartcatchup")
        self.assertQueue(6)

        frozen_datetime.move_to("2020-01-05")
        management.call_command("worker", "--until_done")

        # three day passed, catch_up should have run thrice and other once
        self.assertQueue(2, task_name="normal")
        self.assertQueue(3, task_name="autostart")
        self.assertQueue(4, task_name="catchup")
        self.assertQueue(5, task_name="autostartcatchup")
        self.assertQueue(14)

        # make sure all tasks succeeded
        self.assertQueue(14, state=TaskExec.States.SUCCEEDED)

        # make sure we got correct dates
        self.assertResults(
            task_name="normal",
            expected=[
                "2020-01-01 12:00",
                "2020-01-04 12:00",
            ],
        )
        self.assertResults(
            task_name="autostart",
            expected=[
                "2019-12-31 12:00",
                "2020-01-01 12:00",
                "2020-01-04 12:00",
            ],
        )
        self.assertResults(
            task_name="catchup",
            expected=[
                "2020-01-01 12:00",
                "2020-01-02 12:00",
                "2020-01-03 12:00",
                "2020-01-04 12:00",
            ],
        )
        self.assertResults(
            task_name="autostartcatchup",
            expected=[
                "2019-12-31 12:00",
                "2020-01-01 12:00",
                "2020-01-02 12:00",
                "2020-01-03 12:00",
                "2020-01-04 12:00",
            ],
        )

    @freeze_time("2020-01-01", as_kwarg="frozen_datetime")
    def test_invalid_schedule(self, frozen_datetime):
        """Testing invalid schedules"""

        @schedule_task(cron="0 * * * *")
        @register_task(name="valid")
        def a():
            return f"Valid task"

        @schedule_task(cron="0 * * * *")
        @register_task(name="invalid")
        def a():
            return f"Invalid task"

        all_schedules = ScheduleExec.objects.all()

        management.call_command("worker", "--until_done")

        self.assertEqual(
            all_schedules.filter(state=ScheduleExec.States.ACTIVE).count(), 2
        )
        self.assertEqual(
            all_schedules.filter(state=ScheduleExec.States.INVALID).count(), 0
        )
        self.assertEqual(all_schedules.count(), 2)

        del schedules_registry["invalid"]

        management.call_command("worker", "--until_done")

        self.assertEqual(
            all_schedules.filter(state=ScheduleExec.States.ACTIVE).count(), 1
        )
        self.assertEqual(
            all_schedules.filter(state=ScheduleExec.States.INVALID).count(), 1
        )
        self.assertEqual(all_schedules.count(), 2)

    def test_named_queues(self):
        """Checking named queues"""

        @schedule_task(cron="* * * * *")  # queue="default"
        @register_task(name="a")
        def a(x):
            return x * 2

        @schedule_task(cron="* * * * *", queue="queue_b")
        @register_task(name="b")
        def b(x):
            return x * 2

        @schedule_task(cron="* * * * *", queue="queue_c")
        @register_task(name="c")
        def c(x):
            return x * 2

        @schedule_task(cron="* * * * *", queue="queue_d")
        @register_task(name="d")
        def d(x):
            return x * 2

        @schedule_task(cron="* * * * *", queue="queue_e")
        @register_task(name="e")
        def e(x):
            return x * 2

        @schedule_task(cron="* * * * *", queue="queue_f")
        @register_task(name="f")
        def f(x):
            return x * 2

        @schedule_task(cron="* * * * *", queue="queue_g")
        @register_task(name="g")
        def g(x):
            return x * 2

        @schedule_task(cron="* * * * *", queue="queue_h")
        @register_task(name="h")
        def h(x):
            return x * 2

        self.assertSchedule("a", None)
        self.assertSchedule("b", None)
        self.assertSchedule("c", None)
        self.assertSchedule("d", None)
        self.assertSchedule("e", None)
        self.assertSchedule("f", None)
        self.assertSchedule("g", None)
        self.assertSchedule("h", None)

        # make sure schedules get assigned to default queue by default
        management.call_command("worker", "--until_done", "--queue", "default")

        self.assertSchedule("a", ScheduleExec.States.ACTIVE)
        self.assertSchedule("b", None)
        self.assertSchedule("c", None)
        self.assertSchedule("d", None)
        self.assertSchedule("e", None)
        self.assertSchedule("f", None)
        self.assertSchedule("g", None)
        self.assertSchedule("h", None)

        # make sure worker only runs their queue
        management.call_command("worker", "--until_done", "--queue", "queue_c")

        self.assertSchedule("a", ScheduleExec.States.ACTIVE)
        self.assertSchedule("b", None)
        self.assertSchedule("c", ScheduleExec.States.ACTIVE)
        self.assertSchedule("d", None)
        self.assertSchedule("e", None)
        self.assertSchedule("f", None)
        self.assertSchedule("g", None)
        self.assertSchedule("h", None)

        # make sure worker can run multiple queues
        management.call_command(
            "worker", "--until_done", "--queue", "queue_b", "--queue", "queue_d"
        )

        self.assertSchedule("a", ScheduleExec.States.ACTIVE)
        self.assertSchedule("b", ScheduleExec.States.ACTIVE)
        self.assertSchedule("c", ScheduleExec.States.ACTIVE)
        self.assertSchedule("d", ScheduleExec.States.ACTIVE)
        self.assertSchedule("e", None)
        self.assertSchedule("f", None)
        self.assertSchedule("g", None)
        self.assertSchedule("h", None)

        # make sure worker exclude queue works
        management.call_command(
            "worker",
            "--until_done",
            "--exclude_queue",
            "queue_g",
            "--exclude_queue",
            "queue_h",
        )

        self.assertSchedule("a", ScheduleExec.States.ACTIVE)
        self.assertSchedule("b", ScheduleExec.States.ACTIVE)
        self.assertSchedule("c", ScheduleExec.States.ACTIVE)
        self.assertSchedule("d", ScheduleExec.States.ACTIVE)
        self.assertSchedule("e", ScheduleExec.States.ACTIVE)
        self.assertSchedule("f", ScheduleExec.States.ACTIVE)
        self.assertSchedule("g", None)
        self.assertSchedule("h", None)

        # make sure worker run all queues by default
        management.call_command("worker", "--until_done")

        self.assertSchedule("a", ScheduleExec.States.ACTIVE)
        self.assertSchedule("b", ScheduleExec.States.ACTIVE)
        self.assertSchedule("c", ScheduleExec.States.ACTIVE)
        self.assertSchedule("d", ScheduleExec.States.ACTIVE)
        self.assertSchedule("e", ScheduleExec.States.ACTIVE)
        self.assertSchedule("f", ScheduleExec.States.ACTIVE)
        self.assertSchedule("g", ScheduleExec.States.ACTIVE)
        self.assertSchedule("h", ScheduleExec.States.ACTIVE)
