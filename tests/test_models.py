import datetime

from django.core import management
from django.test import TestCase
from django.utils import timezone
from freezegun import freeze_time

from django_toosimple_q.decorators import register_task, schedule_task
from django_toosimple_q.models import ScheduleExec, TaskExec
from django_toosimple_q.registry import schedules

from .utils import EmptyRegistryMixin, QueueAssertionMixin


class TestCore(QueueAssertionMixin, EmptyRegistryMixin, TestCase):
    def test_task_states(self):
        """Checking correctness of task states"""

        # Succeeding task
        @register_task(name="a")
        def a(x):
            return x * 2

        t = a.queue(2)
        self.assertEqual(t.state, TaskExec.QUEUED)
        t.execute()
        self.assertEqual(t.state, TaskExec.SUCCEEDED)
        self.assertEqual(t.result, 4)

        # Failing task
        @register_task(name="b")
        def b(x):
            return x / 0

        t = TaskExec.objects.create(function="b", args=[2])
        self.assertEqual(t.state, TaskExec.QUEUED)
        t.execute()
        self.assertEqual(t.state, TaskExec.FAILED)

    def test_task_registration(self):
        """Checking task registration"""

        # We cannot run arbitrary functions
        t = TaskExec.objects.create(function="print", args=["test"])
        self.assertEqual(t.state, TaskExec.QUEUED)
        t.execute()
        self.assertEqual(t.state, TaskExec.INVALID)

        # We can run registered functions
        @register_task(name="a")
        def a(x):
            pass

        t = TaskExec.objects.create(function="a", args=["test"])
        self.assertEqual(t.state, TaskExec.QUEUED)
        t.execute()
        self.assertEqual(t.state, TaskExec.SUCCEEDED)

    def test_task_queuing(self):
        """Checking task queuing"""

        @register_task(name="a")
        def a(x):
            return x * 2

        a.queue(1)

        self.assertQueue(1, state=TaskExec.QUEUED)
        self.assertQueue(0, state=TaskExec.SUCCEEDED)
        self.assertQueue(1)

        a.queue(2)

        self.assertQueue(2, state=TaskExec.QUEUED)
        self.assertQueue(0, state=TaskExec.SUCCEEDED)
        self.assertQueue(2)

        t = a.queue(3)
        t.state = TaskExec.SLEEPING
        t.due = timezone.now() + datetime.timedelta(hours=1)
        t.save()

        self.assertQueue(1, state=TaskExec.SLEEPING)
        self.assertQueue(2, state=TaskExec.QUEUED)
        self.assertQueue(0, state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(1, state=TaskExec.SLEEPING)
        self.assertQueue(1, state=TaskExec.QUEUED)
        self.assertQueue(1, state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(1, state=TaskExec.SLEEPING)
        self.assertQueue(0, state=TaskExec.QUEUED)
        self.assertQueue(2, state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(1, state=TaskExec.SLEEPING)
        self.assertQueue(0, state=TaskExec.QUEUED)
        self.assertQueue(2, state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

    def test_task_queuing_with_priorities(self):
        """Checking task queuing with priorities"""

        @register_task(name="p2", priority=2)
        def p2(x):
            return x * 2

        @register_task(name="p1a", priority=1)
        def p1a(x):
            return x * 2

        @register_task(name="p1b", priority=1)
        def p1b(x):
            return x * 2

        p1a.queue(1)
        p1b.queue(1)
        p2.queue(1)

        self.assertQueue(1, function="p2", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1a", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1b", state=TaskExec.QUEUED)
        self.assertQueue(0, function="p2", state=TaskExec.SUCCEEDED)
        self.assertQueue(0, function="p1a", state=TaskExec.SUCCEEDED)
        self.assertQueue(0, function="p1b", state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1a", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1b", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p2", state=TaskExec.SUCCEEDED)
        self.assertQueue(0, function="p1a", state=TaskExec.SUCCEEDED)
        self.assertQueue(0, function="p1b", state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=TaskExec.QUEUED)
        self.assertQueue(0, function="p1a", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1b", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p2", state=TaskExec.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=TaskExec.SUCCEEDED)
        self.assertQueue(0, function="p1b", state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=TaskExec.QUEUED)
        self.assertQueue(0, function="p1a", state=TaskExec.QUEUED)
        self.assertQueue(0, function="p1b", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p2", state=TaskExec.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=TaskExec.SUCCEEDED)
        self.assertQueue(1, function="p1b", state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        p2.queue(1)
        p1b.queue(1)
        p1a.queue(1)

        self.assertQueue(1, function="p2", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1a", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1b", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p2", state=TaskExec.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=TaskExec.SUCCEEDED)
        self.assertQueue(1, function="p1b", state=TaskExec.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1a", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1b", state=TaskExec.QUEUED)
        self.assertQueue(2, function="p2", state=TaskExec.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=TaskExec.SUCCEEDED)
        self.assertQueue(1, function="p1b", state=TaskExec.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=TaskExec.QUEUED)
        self.assertQueue(1, function="p1a", state=TaskExec.QUEUED)
        self.assertQueue(0, function="p1b", state=TaskExec.QUEUED)
        self.assertQueue(2, function="p2", state=TaskExec.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=TaskExec.SUCCEEDED)
        self.assertQueue(2, function="p1b", state=TaskExec.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=TaskExec.QUEUED)
        self.assertQueue(0, function="p1a", state=TaskExec.QUEUED)
        self.assertQueue(0, function="p1b", state=TaskExec.QUEUED)
        self.assertQueue(2, function="p2", state=TaskExec.SUCCEEDED)
        self.assertQueue(2, function="p1a", state=TaskExec.SUCCEEDED)
        self.assertQueue(2, function="p1b", state=TaskExec.SUCCEEDED)
        self.assertQueue(6)

    def test_task_queuing_with_unique(self):
        """Checking task queuing with unique"""

        @register_task(name="normal")
        def normal(x):
            return x * 2

        @register_task(name="unique", unique=True)
        def unique(x):
            return x * 2

        self.assertQueue(0)

        normal.queue(1)
        normal.queue(1)
        unique.queue(1)
        unique.queue(1)

        self.assertQueue(1, function="unique", state=TaskExec.QUEUED)
        self.assertQueue(2, function="normal", state=TaskExec.QUEUED)
        self.assertQueue(0, function="unique", state=TaskExec.SUCCEEDED)
        self.assertQueue(0, function="normal", state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="unique", state=TaskExec.QUEUED)
        self.assertQueue(0, function="normal", state=TaskExec.QUEUED)
        self.assertQueue(1, function="unique", state=TaskExec.SUCCEEDED)
        self.assertQueue(2, function="normal", state=TaskExec.SUCCEEDED)
        self.assertQueue(3)

        normal.queue(1)
        normal.queue(1)
        unique.queue(1)
        unique.queue(1)

        self.assertQueue(1, function="unique", state=TaskExec.QUEUED)
        self.assertQueue(2, function="normal", state=TaskExec.QUEUED)
        self.assertQueue(1, function="unique", state=TaskExec.SUCCEEDED)
        self.assertQueue(2, function="normal", state=TaskExec.SUCCEEDED)
        self.assertQueue(6)

    def test_task_retries(self):
        """Checking task retries"""

        @register_task(name="div_zero", retries=10)
        def div_zero(x):
            return x / 0

        self.assertQueue(0)

        div_zero.queue(1)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(0, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(1, function="div_zero", state=TaskExec.FAILED, replaced=False)
        self.assertQueue(10, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(11)

    @freeze_time("2020-01-01", as_kwarg="frozen_datetime")
    def test_task_retries_delay(self, frozen_datetime):
        """Checking task retries with delay"""

        initial_datetime = datetime.datetime.now()

        @register_task(name="div_zero", retries=10, retry_delay=10)
        def div_zero(x):
            return x / 0

        self.assertQueue(0)

        div_zero.queue(1)

        self.assertQueue(1)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(1, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(1, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(2)
        self.assertEqual(TaskExec.objects.last().retries, 9)
        self.assertEqual(TaskExec.objects.last().retry_delay, 20)

        # if we don't wait, no further task will be processed
        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(1, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(1, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(2)
        self.assertEqual(TaskExec.objects.last().retries, 9)
        self.assertEqual(TaskExec.objects.last().retry_delay, 20)

        # if we wait a bit more than 10 seconds, one retry will be done
        frozen_datetime.move_to(initial_datetime + datetime.timedelta(seconds=11))

        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(2, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(1, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(3)
        self.assertEqual(TaskExec.objects.last().retries, 8)
        self.assertEqual(TaskExec.objects.last().retry_delay, 40)

        # if we wait a bit more than 10+20, then 10+20+40 seconds, two more retries will be done
        frozen_datetime.move_to(initial_datetime + datetime.timedelta(seconds=31))
        management.call_command("worker", "--until_done")
        frozen_datetime.move_to(initial_datetime + datetime.timedelta(seconds=71))
        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(4, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(1, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(5)
        self.assertEqual(TaskExec.objects.last().retries, 6)
        self.assertEqual(TaskExec.objects.last().retry_delay, 160)

    @freeze_time("2020-01-01", as_kwarg="frozen_datetime")
    def test_task_retries_delay_unique(self, frozen_datetime):
        """Checking unique task retries with delay"""

        @register_task(name="div_zero", retries=10, retry_delay=10, unique=True)
        def div_zero(x):
            return x / 0

        self.assertQueue(0)

        div_zero.queue(1)

        self.assertQueue(1)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(1, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(1, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(2)
        self.assertEqual(TaskExec.objects.last().retries, 9)
        self.assertEqual(TaskExec.objects.last().retry_delay, 20)

        # if we don't wait, no further task will be processed
        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(1, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(1, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(2)
        self.assertEqual(TaskExec.objects.last().retries, 9)
        self.assertEqual(TaskExec.objects.last().retry_delay, 20)

        # if we requeue the task, it will be run immediatly
        div_zero.queue(1)
        self.assertQueue(2)

        self.assertQueue(1, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(1, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(0, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(2)
        self.assertEqual(TaskExec.objects.last().retries, 9)
        self.assertEqual(TaskExec.objects.last().retry_delay, 20)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=TaskExec.QUEUED)
        self.assertQueue(2, function="div_zero", state=TaskExec.FAILED, replaced=True)
        self.assertQueue(1, function="div_zero", state=TaskExec.SLEEPING)
        self.assertQueue(3)
        self.assertEqual(TaskExec.objects.last().retries, 8)
        self.assertEqual(TaskExec.objects.last().retry_delay, 40)

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

        self.assertEquals(len(schedules), 4)
        self.assertEquals(ScheduleExec.objects.count(), 0)
        self.assertQueue(0)

        management.call_command("worker", "--until_done")

        # first run, only tasks with run_on_creation=True should run as no time passed
        self.assertQueue(0, function="normal")
        self.assertQueue(1, function="autostart")
        self.assertQueue(0, function="catchup")
        self.assertQueue(1, function="autostartcatchup")
        self.assertQueue(2)

        management.call_command("worker", "--until_done")

        # second run, no time passed so no change
        self.assertQueue(0, function="normal")
        self.assertQueue(1, function="autostart")
        self.assertQueue(0, function="catchup")
        self.assertQueue(1, function="autostartcatchup")
        self.assertQueue(2)

        frozen_datetime.move_to("2020-01-02")
        management.call_command("worker", "--until_done")

        # one day passed, all tasks should have run once
        self.assertQueue(1, function="normal")
        self.assertQueue(2, function="autostart")
        self.assertQueue(1, function="catchup")
        self.assertQueue(2, function="autostartcatchup")
        self.assertQueue(6)

        frozen_datetime.move_to("2020-01-05")
        management.call_command("worker", "--until_done")

        # three day passed, catch_up should have run thrice and other once
        self.assertQueue(2, function="normal")
        self.assertQueue(3, function="autostart")
        self.assertQueue(4, function="catchup")
        self.assertQueue(5, function="autostartcatchup")
        self.assertQueue(14)

        # make sure all tasks succeeded
        self.assertQueue(14, state=TaskExec.SUCCEEDED)

        # make sure we got correct dates
        def results_list(function_name):
            return list(
                TaskExec.objects.order_by("created")
                .filter(function=function_name)
                .values_list("result", flat=True)
            )

        self.assertEqual(
            results_list("normal"),
            [
                "2020-01-01 12:00",
                "2020-01-04 12:00",
            ],
        )
        self.assertEqual(
            results_list("autostart"),
            [
                "2019-12-31 12:00",
                "2020-01-01 12:00",
                "2020-01-04 12:00",
            ],
        )
        self.assertEqual(
            results_list("catchup"),
            [
                "2020-01-01 12:00",
                "2020-01-02 12:00",
                "2020-01-03 12:00",
                "2020-01-04 12:00",
            ],
        )
        self.assertEqual(
            results_list("autostartcatchup"),
            [
                "2019-12-31 12:00",
                "2020-01-01 12:00",
                "2020-01-02 12:00",
                "2020-01-03 12:00",
                "2020-01-04 12:00",
            ],
        )

    def test_named_queues(self):
        """Checking named queues"""

        @register_task(name="a")  # queue="default"
        def a(x):
            return x * 2

        @register_task(name="b", queue="queue_b")
        def b(x):
            return x * 2

        @register_task(name="c", queue="queue_c")
        def c(x):
            return x * 2

        @register_task(name="d", queue="queue_d")
        def d(x):
            return x * 2

        @register_task(name="e", queue="queue_e")
        def e(x):
            return x * 2

        @register_task(name="f", queue="queue_f")
        def f(x):
            return x * 2

        @register_task(name="g", queue="queue_g")
        def g(x):
            return x * 2

        @register_task(name="h", queue="queue_h")
        def h(x):
            return x * 2

        task_a = a.queue(1)
        task_b = b.queue(1)
        task_c = c.queue(1)
        task_d = d.queue(1)
        task_e = e.queue(1)
        task_f = f.queue(1)
        task_g = g.queue(1)
        task_h = h.queue(1)

        self.assertTask(task_a, TaskExec.QUEUED)
        self.assertTask(task_b, TaskExec.QUEUED)
        self.assertTask(task_c, TaskExec.QUEUED)
        self.assertTask(task_d, TaskExec.QUEUED)
        self.assertTask(task_e, TaskExec.QUEUED)
        self.assertTask(task_f, TaskExec.QUEUED)
        self.assertTask(task_g, TaskExec.QUEUED)
        self.assertTask(task_h, TaskExec.QUEUED)

        # make sure tasks get assigned to default queue by default
        management.call_command("worker", "--until_done", "--queue", "default")

        self.assertTask(task_a, TaskExec.SUCCEEDED)
        self.assertTask(task_b, TaskExec.QUEUED)
        self.assertTask(task_c, TaskExec.QUEUED)
        self.assertTask(task_d, TaskExec.QUEUED)
        self.assertTask(task_e, TaskExec.QUEUED)
        self.assertTask(task_f, TaskExec.QUEUED)
        self.assertTask(task_g, TaskExec.QUEUED)
        self.assertTask(task_h, TaskExec.QUEUED)

        # make sure worker only runs their queue
        management.call_command("worker", "--until_done", "--queue", "queue_c")

        self.assertTask(task_a, TaskExec.SUCCEEDED)
        self.assertTask(task_b, TaskExec.QUEUED)
        self.assertTask(task_c, TaskExec.SUCCEEDED)
        self.assertTask(task_d, TaskExec.QUEUED)
        self.assertTask(task_e, TaskExec.QUEUED)
        self.assertTask(task_f, TaskExec.QUEUED)
        self.assertTask(task_g, TaskExec.QUEUED)
        self.assertTask(task_h, TaskExec.QUEUED)

        # make sure worker can run multiple queues
        management.call_command(
            "worker", "--until_done", "--queue", "queue_b", "--queue", "queue_d"
        )

        self.assertTask(task_a, TaskExec.SUCCEEDED)
        self.assertTask(task_b, TaskExec.SUCCEEDED)
        self.assertTask(task_c, TaskExec.SUCCEEDED)
        self.assertTask(task_d, TaskExec.SUCCEEDED)
        self.assertTask(task_e, TaskExec.QUEUED)
        self.assertTask(task_f, TaskExec.QUEUED)
        self.assertTask(task_g, TaskExec.QUEUED)
        self.assertTask(task_h, TaskExec.QUEUED)

        # make sure worker exclude queue works
        management.call_command(
            "worker",
            "--until_done",
            "--exclude_queue",
            "queue_g",
            "--exclude_queue",
            "queue_h",
        )

        self.assertTask(task_a, TaskExec.SUCCEEDED)
        self.assertTask(task_b, TaskExec.SUCCEEDED)
        self.assertTask(task_c, TaskExec.SUCCEEDED)
        self.assertTask(task_d, TaskExec.SUCCEEDED)
        self.assertTask(task_e, TaskExec.SUCCEEDED)
        self.assertTask(task_f, TaskExec.SUCCEEDED)
        self.assertTask(task_g, TaskExec.QUEUED)
        self.assertTask(task_h, TaskExec.QUEUED)

        # make sure worker run all queues by default
        management.call_command("worker", "--until_done")

        self.assertTask(task_a, TaskExec.SUCCEEDED)
        self.assertTask(task_b, TaskExec.SUCCEEDED)
        self.assertTask(task_c, TaskExec.SUCCEEDED)
        self.assertTask(task_d, TaskExec.SUCCEEDED)
        self.assertTask(task_e, TaskExec.SUCCEEDED)
        self.assertTask(task_f, TaskExec.SUCCEEDED)
        self.assertTask(task_g, TaskExec.SUCCEEDED)
        self.assertTask(task_h, TaskExec.SUCCEEDED)
