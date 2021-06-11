import datetime

from django.core import management
from django.test import TestCase
from django.utils import timezone
from freezegun import freeze_time
from pytz import UTC

from django_toosimple_q.decorators import register_task, schedule
from django_toosimple_q.models import Schedule, Task
from django_toosimple_q.registry import schedules, tasks


class TestDjango_toosimple_q(TestCase):
    def setUp(self):
        schedules.clear()
        tasks.clear()

    def tearDown(self):
        pass

    def assertQueue(self, count, function=None, state=None):
        tasks = Task.objects.all()
        if function:
            tasks = tasks.filter(function=function)
        if state:
            tasks = tasks.filter(state=state)
        self.assertEquals(tasks.count(), count)

    def assertTask(self, task, state):
        self.assertEquals(Task.objects.get(pk=task.pk).state, state)

    def test_task_states(self):
        """Checking correctness of task states"""

        # Succeeding task
        @register_task("a")
        def a(x):
            return x * 2

        t = Task.objects.create(function="a", args=[2])
        self.assertEqual(t.state, Task.QUEUED)
        t.execute()
        self.assertEqual(t.state, Task.SUCCEEDED)
        self.assertEqual(t.result, 4)

        # Failing task
        @register_task("b")
        def b(x):
            return x / 0

        t = Task.objects.create(function="b", args=[2])
        self.assertEqual(t.state, Task.QUEUED)
        t.execute()
        self.assertEqual(t.state, Task.FAILED)

    def test_task_registration(self):
        """Checking task registration"""

        # We cannot run arbitrary functions
        t = Task.objects.create(function="print", args=["test"])
        self.assertEqual(t.state, Task.QUEUED)
        t.execute()
        self.assertEqual(t.state, Task.INVALID)

        # We can run registered functions
        @register_task("a")
        def a(x):
            pass

        t = Task.objects.create(function="a", args=["test"])
        self.assertEqual(t.state, Task.QUEUED)
        t.execute()
        self.assertEqual(t.state, Task.SUCCEEDED)

    def test_task_queuing(self):
        """Checking task queuing"""

        @register_task("a")
        def a(x):
            return x * 2

        a.queue(1)

        self.assertQueue(1, state=Task.QUEUED)
        self.assertQueue(0, state=Task.SUCCEEDED)
        self.assertQueue(1)

        a.queue(2)

        self.assertQueue(2, state=Task.QUEUED)
        self.assertQueue(0, state=Task.SUCCEEDED)
        self.assertQueue(2)

        t = a.queue(3)
        t.state = Task.SLEEPING
        t.due = timezone.now() + datetime.timedelta(hours=1)
        t.save()

        self.assertQueue(1, state=Task.SLEEPING)
        self.assertQueue(2, state=Task.QUEUED)
        self.assertQueue(0, state=Task.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(1, state=Task.SLEEPING)
        self.assertQueue(1, state=Task.QUEUED)
        self.assertQueue(1, state=Task.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(1, state=Task.SLEEPING)
        self.assertQueue(0, state=Task.QUEUED)
        self.assertQueue(2, state=Task.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(1, state=Task.SLEEPING)
        self.assertQueue(0, state=Task.QUEUED)
        self.assertQueue(2, state=Task.SUCCEEDED)
        self.assertQueue(3)

    def test_task_queuing_with_priorities(self):
        """Checking task queuing with priorities"""

        @register_task("p2", priority=2)
        def p2(x):
            return x * 2

        @register_task("p1a", priority=1)
        def p1a(x):
            return x * 2

        @register_task("p1b", priority=1)
        def p1b(x):
            return x * 2

        p1a.queue(1)
        p1b.queue(1)
        p2.queue(1)

        self.assertQueue(1, function="p2", state=Task.QUEUED)
        self.assertQueue(1, function="p1a", state=Task.QUEUED)
        self.assertQueue(1, function="p1b", state=Task.QUEUED)
        self.assertQueue(0, function="p2", state=Task.SUCCEEDED)
        self.assertQueue(0, function="p1a", state=Task.SUCCEEDED)
        self.assertQueue(0, function="p1b", state=Task.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=Task.QUEUED)
        self.assertQueue(1, function="p1a", state=Task.QUEUED)
        self.assertQueue(1, function="p1b", state=Task.QUEUED)
        self.assertQueue(1, function="p2", state=Task.SUCCEEDED)
        self.assertQueue(0, function="p1a", state=Task.SUCCEEDED)
        self.assertQueue(0, function="p1b", state=Task.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=Task.QUEUED)
        self.assertQueue(0, function="p1a", state=Task.QUEUED)
        self.assertQueue(1, function="p1b", state=Task.QUEUED)
        self.assertQueue(1, function="p2", state=Task.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=Task.SUCCEEDED)
        self.assertQueue(0, function="p1b", state=Task.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=Task.QUEUED)
        self.assertQueue(0, function="p1a", state=Task.QUEUED)
        self.assertQueue(0, function="p1b", state=Task.QUEUED)
        self.assertQueue(1, function="p2", state=Task.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=Task.SUCCEEDED)
        self.assertQueue(1, function="p1b", state=Task.SUCCEEDED)
        self.assertQueue(3)

        p2.queue(1)
        p1b.queue(1)
        p1a.queue(1)

        self.assertQueue(1, function="p2", state=Task.QUEUED)
        self.assertQueue(1, function="p1a", state=Task.QUEUED)
        self.assertQueue(1, function="p1b", state=Task.QUEUED)
        self.assertQueue(1, function="p2", state=Task.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=Task.SUCCEEDED)
        self.assertQueue(1, function="p1b", state=Task.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=Task.QUEUED)
        self.assertQueue(1, function="p1a", state=Task.QUEUED)
        self.assertQueue(1, function="p1b", state=Task.QUEUED)
        self.assertQueue(2, function="p2", state=Task.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=Task.SUCCEEDED)
        self.assertQueue(1, function="p1b", state=Task.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=Task.QUEUED)
        self.assertQueue(1, function="p1a", state=Task.QUEUED)
        self.assertQueue(0, function="p1b", state=Task.QUEUED)
        self.assertQueue(2, function="p2", state=Task.SUCCEEDED)
        self.assertQueue(1, function="p1a", state=Task.SUCCEEDED)
        self.assertQueue(2, function="p1b", state=Task.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, function="p2", state=Task.QUEUED)
        self.assertQueue(0, function="p1a", state=Task.QUEUED)
        self.assertQueue(0, function="p1b", state=Task.QUEUED)
        self.assertQueue(2, function="p2", state=Task.SUCCEEDED)
        self.assertQueue(2, function="p1a", state=Task.SUCCEEDED)
        self.assertQueue(2, function="p1b", state=Task.SUCCEEDED)
        self.assertQueue(6)

    def test_task_queuing_with_unique(self):
        """Checking task queuing with unique"""

        @register_task("normal")
        def normal(x):
            return x * 2

        @register_task("unique", unique=True)
        def unique(x):
            return x * 2

        self.assertQueue(0)

        normal.queue(1)
        normal.queue(1)
        unique.queue(1)
        unique.queue(1)

        self.assertQueue(1, function="unique", state=Task.QUEUED)
        self.assertQueue(2, function="normal", state=Task.QUEUED)
        self.assertQueue(0, function="unique", state=Task.SUCCEEDED)
        self.assertQueue(0, function="normal", state=Task.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="unique", state=Task.QUEUED)
        self.assertQueue(0, function="normal", state=Task.QUEUED)
        self.assertQueue(1, function="unique", state=Task.SUCCEEDED)
        self.assertQueue(2, function="normal", state=Task.SUCCEEDED)
        self.assertQueue(3)

        normal.queue(1)
        normal.queue(1)
        unique.queue(1)
        unique.queue(1)

        self.assertQueue(1, function="unique", state=Task.QUEUED)
        self.assertQueue(2, function="normal", state=Task.QUEUED)
        self.assertQueue(1, function="unique", state=Task.SUCCEEDED)
        self.assertQueue(2, function="normal", state=Task.SUCCEEDED)
        self.assertQueue(6)

    def test_task_retries(self):
        """Checking task retries"""

        @register_task("div_zero", retries=10)
        def div_zero(x):
            return x / 0

        self.assertQueue(0)

        div_zero.queue(1)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, function="div_zero", state=Task.QUEUED)
        self.assertQueue(0, function="div_zero", state=Task.SLEEPING)
        self.assertQueue(10, function="div_zero", state=Task.RETRIED)
        self.assertQueue(1, function="div_zero", state=Task.FAILED)
        self.assertQueue(11)

    def test_task_retries_delay(self):
        """Checking task retries with delay"""

        @register_task("div_zero", retries=10, retry_delay=10)
        def div_zero(x):
            return x / 0

        # # TODO : use decorator instead once https://github.com/spulec/freezegun/issues/262 is fixed
        initial_datetime = datetime.datetime(year=2020, month=1, day=1)
        with freeze_time(initial_datetime) as frozen_datetime:

            self.assertQueue(0)

            div_zero.queue(1)

            self.assertQueue(1)

            management.call_command("worker", "--until_done")

            self.assertQueue(0, function="div_zero", state=Task.QUEUED)
            self.assertQueue(1, function="div_zero", state=Task.RETRIED)
            self.assertQueue(1, function="div_zero", state=Task.SLEEPING)
            self.assertQueue(2)
            self.assertEqual(Task.objects.last().retries, 9)
            self.assertEqual(Task.objects.last().retry_delay, 20)

            # if we don't wait, no further task will be processed
            management.call_command("worker", "--until_done")

            self.assertQueue(0, function="div_zero", state=Task.QUEUED)
            self.assertQueue(1, function="div_zero", state=Task.RETRIED)
            self.assertQueue(1, function="div_zero", state=Task.SLEEPING)
            self.assertQueue(2)
            self.assertEqual(Task.objects.last().retries, 9)
            self.assertEqual(Task.objects.last().retry_delay, 20)

            # if we wait a bit more than 10 seconds, one retry will be done
            frozen_datetime.move_to(initial_datetime + datetime.timedelta(seconds=11))

            management.call_command("worker", "--until_done")

            self.assertQueue(0, function="div_zero", state=Task.QUEUED)
            self.assertQueue(2, function="div_zero", state=Task.RETRIED)
            self.assertQueue(1, function="div_zero", state=Task.SLEEPING)
            self.assertQueue(3)
            self.assertEqual(Task.objects.last().retries, 8)
            self.assertEqual(Task.objects.last().retry_delay, 40)

            # if we wait a bit more than 10+20, then 10+20+40 seconds, two more retries will be done
            frozen_datetime.move_to(initial_datetime + datetime.timedelta(seconds=31))
            management.call_command("worker", "--until_done")
            frozen_datetime.move_to(initial_datetime + datetime.timedelta(seconds=71))
            management.call_command("worker", "--until_done")

            self.assertQueue(0, function="div_zero", state=Task.QUEUED)
            self.assertQueue(4, function="div_zero", state=Task.RETRIED)
            self.assertQueue(1, function="div_zero", state=Task.SLEEPING)
            self.assertQueue(5)
            self.assertEqual(Task.objects.last().retries, 6)
            self.assertEqual(Task.objects.last().retry_delay, 160)

    def test_task_retries_delay_unique(self):
        """Checking unique task retries with delay"""

        @register_task("div_zero", retries=10, retry_delay=10, unique=True)
        def div_zero(x):
            return x / 0

        # # TODO : use decorator instead once https://github.com/spulec/freezegun/issues/262 is fixed
        initial_datetime = datetime.datetime(year=2020, month=1, day=1)
        with freeze_time(initial_datetime) as frozen_datetime:

            self.assertQueue(0)

            div_zero.queue(1)

            self.assertQueue(1)

            management.call_command("worker", "--until_done")

            self.assertQueue(0, function="div_zero", state=Task.QUEUED)
            self.assertQueue(1, function="div_zero", state=Task.RETRIED)
            self.assertQueue(1, function="div_zero", state=Task.SLEEPING)
            self.assertQueue(2)
            self.assertEqual(Task.objects.last().retries, 9)
            self.assertEqual(Task.objects.last().retry_delay, 20)

            # if we don't wait, no further task will be processed
            management.call_command("worker", "--until_done")

            self.assertQueue(0, function="div_zero", state=Task.QUEUED)
            self.assertQueue(1, function="div_zero", state=Task.RETRIED)
            self.assertQueue(1, function="div_zero", state=Task.SLEEPING)
            self.assertQueue(2)
            self.assertEqual(Task.objects.last().retries, 9)
            self.assertEqual(Task.objects.last().retry_delay, 20)

            # if we requeue the task, it will be run immediatly
            div_zero.queue(1)
            self.assertQueue(2)

            self.assertQueue(1, function="div_zero", state=Task.QUEUED)
            self.assertQueue(1, function="div_zero", state=Task.RETRIED)
            self.assertQueue(0, function="div_zero", state=Task.SLEEPING)
            self.assertQueue(2)
            self.assertEqual(Task.objects.last().retries, 9)
            self.assertEqual(Task.objects.last().retry_delay, 20)

            management.call_command("worker", "--until_done")

            self.assertQueue(0, function="div_zero", state=Task.QUEUED)
            self.assertQueue(2, function="div_zero", state=Task.RETRIED)
            self.assertQueue(1, function="div_zero", state=Task.SLEEPING)
            self.assertQueue(3)
            self.assertEqual(Task.objects.last().retries, 8)
            self.assertEqual(Task.objects.last().retry_delay, 40)

    def test_schedule(self):
        """Testing schedules"""

        # TODO : use decorator instead once https://github.com/spulec/freezegun/issues/262 is fixed
        with freeze_time("2020-01-01") as frozen_datetime:

            @schedule(cron="0 12 * * *")
            @register_task("normal")
            def a():
                pass

            @schedule(cron="0 12 * * *", last_check=None)
            @register_task("autostart")
            def b():
                pass

            @schedule(cron="0 12 * * *", catch_up=True)
            @register_task("catchup")
            def c():
                pass

            @schedule(cron="0 12 * * *", last_check=None, catch_up=True)
            @register_task("autostartcatchup")
            def d():
                pass

            @schedule(
                cron="0 12 * * *",
                last_check=datetime.datetime(2019, 12, 31, tzinfo=UTC),
            )
            @register_task("lastcheck")
            def e():
                pass

            @schedule(
                cron="0 12 * * *",
                last_check=datetime.datetime(2019, 12, 30, tzinfo=UTC),
                catch_up=True,
            )
            @register_task("lastcheckcatchup")
            def f():
                pass

            self.assertEquals(Schedule.objects.count(), 0)
            self.assertQueue(0)

            management.call_command("worker", "--recreate_only")

            self.assertEquals(Schedule.objects.count(), 6)
            self.assertQueue(0)

            management.call_command("worker", "--no_recreate", "--until_done")

            # first run, only tasks with last_check=None should run as no time passed
            self.assertQueue(0, function="normal")
            self.assertQueue(1, function="autostart")
            self.assertQueue(0, function="catchup")
            self.assertQueue(1, function="autostartcatchup")
            self.assertQueue(1, function="lastcheck")
            self.assertQueue(2, function="lastcheckcatchup")
            self.assertQueue(5)

            management.call_command("worker", "--no_recreate", "--until_done")

            # second run, no time passed so no change
            self.assertQueue(0, function="normal")
            self.assertQueue(1, function="autostart")
            self.assertQueue(0, function="catchup")
            self.assertQueue(1, function="autostartcatchup")
            self.assertQueue(1, function="lastcheck")
            self.assertQueue(2, function="lastcheckcatchup")
            self.assertQueue(5)

            frozen_datetime.move_to("2020-01-02")
            management.call_command("worker", "--no_recreate", "--until_done")

            # one day passed, all tasks should have run once
            self.assertQueue(1, function="normal")
            self.assertQueue(2, function="autostart")
            self.assertQueue(1, function="catchup")
            self.assertQueue(2, function="autostartcatchup")
            self.assertQueue(2, function="lastcheck")
            self.assertQueue(3, function="lastcheckcatchup")
            self.assertQueue(11)

            frozen_datetime.move_to("2020-01-05")
            management.call_command("worker", "--no_recreate", "--until_done")

            # three day passed, catch_up should have run thrice and other once
            self.assertQueue(2, function="normal")
            self.assertQueue(3, function="autostart")
            self.assertQueue(4, function="catchup")
            self.assertQueue(5, function="autostartcatchup")
            self.assertQueue(3, function="lastcheck")
            self.assertQueue(6, function="lastcheckcatchup")
            self.assertQueue(23)

            # make sure all tasks succeeded
            self.assertQueue(23, state=Task.SUCCEEDED)

    def test_named_queues(self):
        """Checking named queues"""

        @register_task("a")  # queue="default"
        def a(x):
            return x * 2

        @register_task("b", queue="queue_b")
        def b(x):
            return x * 2

        @register_task("c", queue="queue_c")
        def c(x):
            return x * 2

        @register_task("d", queue="queue_d")
        def d(x):
            return x * 2

        @register_task("e", queue="queue_e")
        def e(x):
            return x * 2

        @register_task("f", queue="queue_f")
        def f(x):
            return x * 2

        @register_task("g", queue="queue_g")
        def g(x):
            return x * 2

        @register_task("h", queue="queue_h")
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

        self.assertTask(task_a, Task.QUEUED)
        self.assertTask(task_b, Task.QUEUED)
        self.assertTask(task_c, Task.QUEUED)
        self.assertTask(task_d, Task.QUEUED)
        self.assertTask(task_e, Task.QUEUED)
        self.assertTask(task_f, Task.QUEUED)
        self.assertTask(task_g, Task.QUEUED)
        self.assertTask(task_h, Task.QUEUED)

        # make sure tasks get assigned to default queue by default
        management.call_command("worker", "--until_done", "--queue", "default")

        self.assertTask(task_a, Task.SUCCEEDED)
        self.assertTask(task_b, Task.QUEUED)
        self.assertTask(task_c, Task.QUEUED)
        self.assertTask(task_d, Task.QUEUED)
        self.assertTask(task_e, Task.QUEUED)
        self.assertTask(task_f, Task.QUEUED)
        self.assertTask(task_g, Task.QUEUED)
        self.assertTask(task_h, Task.QUEUED)

        # make sure worker only runs their queue
        management.call_command("worker", "--until_done", "--queue", "queue_c")

        self.assertTask(task_a, Task.SUCCEEDED)
        self.assertTask(task_b, Task.QUEUED)
        self.assertTask(task_c, Task.SUCCEEDED)
        self.assertTask(task_d, Task.QUEUED)
        self.assertTask(task_e, Task.QUEUED)
        self.assertTask(task_f, Task.QUEUED)
        self.assertTask(task_g, Task.QUEUED)
        self.assertTask(task_h, Task.QUEUED)

        # make sure worker can run multiple queues
        management.call_command(
            "worker", "--until_done", "--queue", "queue_b", "--queue", "queue_d"
        )

        self.assertTask(task_a, Task.SUCCEEDED)
        self.assertTask(task_b, Task.SUCCEEDED)
        self.assertTask(task_c, Task.SUCCEEDED)
        self.assertTask(task_d, Task.SUCCEEDED)
        self.assertTask(task_e, Task.QUEUED)
        self.assertTask(task_f, Task.QUEUED)
        self.assertTask(task_g, Task.QUEUED)
        self.assertTask(task_h, Task.QUEUED)

        # make sure worker exclude queue works
        management.call_command(
            "worker",
            "--until_done",
            "--exclude_queue",
            "queue_g",
            "--exclude_queue",
            "queue_h",
        )

        self.assertTask(task_a, Task.SUCCEEDED)
        self.assertTask(task_b, Task.SUCCEEDED)
        self.assertTask(task_c, Task.SUCCEEDED)
        self.assertTask(task_d, Task.SUCCEEDED)
        self.assertTask(task_e, Task.SUCCEEDED)
        self.assertTask(task_f, Task.SUCCEEDED)
        self.assertTask(task_g, Task.QUEUED)
        self.assertTask(task_h, Task.QUEUED)

        # make sure worker run all queues by default
        management.call_command("worker", "--until_done")

        self.assertTask(task_a, Task.SUCCEEDED)
        self.assertTask(task_b, Task.SUCCEEDED)
        self.assertTask(task_c, Task.SUCCEEDED)
        self.assertTask(task_d, Task.SUCCEEDED)
        self.assertTask(task_e, Task.SUCCEEDED)
        self.assertTask(task_f, Task.SUCCEEDED)
        self.assertTask(task_g, Task.SUCCEEDED)
        self.assertTask(task_h, Task.SUCCEEDED)
