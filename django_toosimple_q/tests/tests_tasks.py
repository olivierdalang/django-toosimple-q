import datetime

from django.core import management
from django.utils import timezone
from freezegun import freeze_time

from django_toosimple_q.decorators import register_task
from django_toosimple_q.models import TaskExec

from .base import TooSimpleQRegularTestCase


class TestTasks(TooSimpleQRegularTestCase):
    def test_task_states(self):
        """Checking correctness of task states"""

        @register_task(name="a")
        def a(x):
            return x * 2

        @register_task(name="b")
        def b(x):
            return x / 0

        # Succeeding task
        t = a.queue(2)
        self.assertEqual(t.state, TaskExec.States.QUEUED)
        management.call_command("worker", "--once")
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t.result, 4)
        self.assertEqual(t.error, None)

        # Failing task
        t = b.queue(2)
        self.assertEqual(t.state, TaskExec.States.QUEUED)
        management.call_command("worker", "--once")
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.FAILED)
        self.assertEqual(t.result, None)
        self.assertNotEqual(t.error, None)

        # Task with due date
        t = a.queue(2, due=timezone.now() - datetime.timedelta(hours=1))
        self.assertEqual(t.state, TaskExec.States.SLEEPING)
        management.call_command("worker", "--once")
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t.result, 4)
        self.assertEqual(t.error, None)

        # Invalid task
        t = TaskExec.objects.create(task_name="invalid")
        self.assertEqual(t.state, TaskExec.States.QUEUED)
        management.call_command("worker", "--once")
        t.refresh_from_db()
        self.assertEqual(t.state, TaskExec.States.INVALID)
        self.assertEqual(t.result, None)
        self.assertEqual(t.error, None)

    def test_task_registration(self):
        """Checking task registration"""

        # We cannot run arbitrary functions
        self.assertQueue(0)
        t = TaskExec.objects.create(task_name="print", args=["test"])
        self.assertQueue(1, state=TaskExec.States.QUEUED)
        management.call_command("worker", "--once")
        self.assertQueue(1, state=TaskExec.States.INVALID)

        # We can run registered functions
        @register_task(name="a")
        def a(x):
            pass

        TaskExec.objects.create(task_name="a", args=["test"])
        self.assertQueue(1, state=TaskExec.States.QUEUED)
        management.call_command("worker", "--once")
        self.assertQueue(1, state=TaskExec.States.SUCCEEDED)

    @freeze_time("2020-01-01", as_kwarg="frozen_datetime")
    def test_task_queuing(self, frozen_datetime):
        """Checking task queuing"""

        @register_task(name="a")
        def a(x):
            return x * 2

        t1 = a.queue(1)
        t2 = a.queue(2)
        t3 = a.queue(3, due=timezone.make_aware(datetime.datetime(2020, 1, 1, 2)))
        t4 = a.queue(4, due=timezone.make_aware(datetime.datetime(2020, 1, 1, 1)))
        self.assertEqual(t1.state, TaskExec.States.QUEUED)
        self.assertEqual(t2.state, TaskExec.States.QUEUED)
        self.assertEqual(t3.state, TaskExec.States.SLEEPING)
        self.assertEqual(t4.state, TaskExec.States.SLEEPING)
        self.assertQueue(4)

        # Run a due task
        management.call_command("worker", "--once")
        t1.refresh_from_db()
        t2.refresh_from_db()
        t3.refresh_from_db()
        t4.refresh_from_db()
        self.assertEqual(t1.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t2.state, TaskExec.States.QUEUED)
        self.assertEqual(t3.state, TaskExec.States.SLEEPING)
        self.assertEqual(t4.state, TaskExec.States.SLEEPING)

        # Run a due task
        management.call_command("worker", "--once")
        t1.refresh_from_db()
        t2.refresh_from_db()
        t3.refresh_from_db()
        t4.refresh_from_db()
        self.assertEqual(t1.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t2.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t3.state, TaskExec.States.SLEEPING)
        self.assertEqual(t4.state, TaskExec.States.SLEEPING)

        # All currently due tasks have been run, nothing happens
        management.call_command("worker", "--once")
        t1.refresh_from_db()
        t2.refresh_from_db()
        t3.refresh_from_db()
        t4.refresh_from_db()
        self.assertEqual(t1.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t2.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t3.state, TaskExec.States.SLEEPING)
        self.assertEqual(t4.state, TaskExec.States.SLEEPING)

        # We move to the future, due tasks are now queued, and the first due one is run
        frozen_datetime.move_to(datetime.datetime(2020, 1, 1, 5))
        management.call_command("worker", "--once")
        t1.refresh_from_db()
        t2.refresh_from_db()
        t3.refresh_from_db()
        t4.refresh_from_db()
        self.assertEqual(t1.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t2.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t3.state, TaskExec.States.QUEUED)
        self.assertEqual(t4.state, TaskExec.States.SUCCEEDED)

        # Now the last one is run too
        management.call_command("worker", "--once")
        t1.refresh_from_db()
        t2.refresh_from_db()
        t3.refresh_from_db()
        t4.refresh_from_db()
        self.assertEqual(t1.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t2.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t3.state, TaskExec.States.SUCCEEDED)
        self.assertEqual(t4.state, TaskExec.States.SUCCEEDED)

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

        self.assertQueue(1, task_name="p2", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1b", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="p2", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, task_name="p1a", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, task_name="p1b", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, task_name="p2", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1b", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p2", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, task_name="p1a", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, task_name="p1b", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, task_name="p2", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="p1a", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1b", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p2", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, task_name="p1b", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--once")

        self.assertQueue(0, task_name="p2", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="p1a", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="p1b", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p2", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, task_name="p1b", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(3)

        p2.queue(1)
        p1b.queue(1)
        p1a.queue(1)

        self.assertQueue(1, task_name="p2", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1b", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p2", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, task_name="p1b", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, task_name="p2", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1b", state=TaskExec.States.QUEUED)
        self.assertQueue(2, task_name="p2", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, task_name="p1b", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, task_name="p2", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="p1b", state=TaskExec.States.QUEUED)
        self.assertQueue(2, task_name="p2", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, task_name="p1a", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(2, task_name="p1b", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(6)

        management.call_command("worker", "--once")

        self.assertQueue(0, task_name="p2", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="p1a", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="p1b", state=TaskExec.States.QUEUED)
        self.assertQueue(2, task_name="p2", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(2, task_name="p1a", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(2, task_name="p1b", state=TaskExec.States.SUCCEEDED)
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

        self.assertQueue(1, task_name="unique", state=TaskExec.States.QUEUED)
        self.assertQueue(2, task_name="normal", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="unique", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, task_name="normal", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(3)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, task_name="unique", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="normal", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="unique", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(2, task_name="normal", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(3)

        normal.queue(1)
        normal.queue(1)
        unique.queue(1)
        unique.queue(1)

        self.assertQueue(1, task_name="unique", state=TaskExec.States.QUEUED)
        self.assertQueue(2, task_name="normal", state=TaskExec.States.QUEUED)
        self.assertQueue(1, task_name="unique", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(2, task_name="normal", state=TaskExec.States.SUCCEEDED)
        self.assertQueue(6)

    def test_task_retries(self):
        """Checking task retries"""

        @register_task(name="div_zero", retries=10)
        def div_zero(x):
            return x / 0

        self.assertQueue(0)

        div_zero.queue(1)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, task_name="div_zero", state=TaskExec.States.QUEUED)
        self.assertQueue(0, task_name="div_zero", state=TaskExec.States.SLEEPING)
        self.assertQueue(
            1, task_name="div_zero", state=TaskExec.States.FAILED, replaced=False
        )
        self.assertQueue(
            10, task_name="div_zero", state=TaskExec.States.FAILED, replaced=True
        )
        self.assertQueue(11)

    @freeze_time("2020-01-01", as_kwarg="frozen_datetime")
    def test_task_retries_delay(self, frozen_datetime):
        """Checking task retries with delay"""

        @register_task(name="div_zero", retries=3, retry_delay=60)
        def div_zero(x):
            return x / 0

        self.assertQueue(0)

        # Create the task
        div_zero.queue(1)
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.QUEUED,
            replaced=False,
            due=datetime.datetime(2020, 1, 1, 0, 0),
        )
        self.assertQueue(1)

        # the task failed, it should be replaced with a task due in the future (+1 min)
        management.call_command("worker", "--until_done")
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 0),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.SLEEPING,
            replaced=False,
            due=datetime.datetime(2020, 1, 1, 0, 1),
        )
        self.assertQueue(2)

        # if we don't wait, no further task will be processed
        management.call_command("worker", "--until_done")
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 0),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.SLEEPING,
            replaced=False,
            due=datetime.datetime(2020, 1, 1, 0, 1),
        )
        self.assertQueue(2)

        # if we wait, one retry will be done ( +1 + 2*+1 = +3min)
        frozen_datetime.move_to(datetime.datetime(2020, 1, 1, 0, 1))
        management.call_command("worker", "--until_done")
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 0),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 1),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.SLEEPING,
            replaced=False,
            due=datetime.datetime(2020, 1, 1, 0, 3),
        )
        self.assertQueue(3)

        # if we wait more, delay continues to increase ( +1 + 2*+1 + 2*2*+1 = +7min)
        frozen_datetime.move_to(datetime.datetime(2020, 1, 1, 0, 3))
        management.call_command("worker", "--until_done")
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 0),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 1),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 3),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.SLEEPING,
            replaced=False,
            due=datetime.datetime(2020, 1, 1, 0, 7),
        )
        self.assertQueue(4)

        # if we wait more, last task runs, but we're out of retries, so no new task
        frozen_datetime.move_to(datetime.datetime(2020, 1, 1, 0, 7))
        management.call_command("worker", "--until_done")
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 0),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 1),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=True,
            due=datetime.datetime(2020, 1, 1, 0, 3),
        )
        self.assertQueue(
            1,
            task_name="div_zero",
            state=TaskExec.States.FAILED,
            replaced=False,
            due=datetime.datetime(2020, 1, 1, 0, 7),
        )
        self.assertQueue(4)

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

        self.assertQueue(0, task_name="div_zero", state=TaskExec.States.QUEUED)
        self.assertQueue(
            1, task_name="div_zero", state=TaskExec.States.FAILED, replaced=True
        )
        self.assertQueue(1, task_name="div_zero", state=TaskExec.States.SLEEPING)
        self.assertQueue(2)
        self.assertEqual(TaskExec.objects.last().retries, 9)
        self.assertEqual(TaskExec.objects.last().retry_delay, 20)

        # if we don't wait, no further task will be processed
        management.call_command("worker", "--until_done")

        self.assertQueue(0, task_name="div_zero", state=TaskExec.States.QUEUED)
        self.assertQueue(
            1, task_name="div_zero", state=TaskExec.States.FAILED, replaced=True
        )
        self.assertQueue(1, task_name="div_zero", state=TaskExec.States.SLEEPING)
        self.assertQueue(2)
        self.assertEqual(TaskExec.objects.last().retries, 9)
        self.assertEqual(TaskExec.objects.last().retry_delay, 20)

        # if we requeue the task, it will be run immediatly
        div_zero.queue(1)
        self.assertQueue(2)

        self.assertQueue(1, task_name="div_zero", state=TaskExec.States.QUEUED)
        self.assertQueue(
            1, task_name="div_zero", state=TaskExec.States.FAILED, replaced=True
        )
        self.assertQueue(0, task_name="div_zero", state=TaskExec.States.SLEEPING)
        self.assertQueue(2)
        self.assertEqual(TaskExec.objects.last().retries, 9)
        self.assertEqual(TaskExec.objects.last().retry_delay, 20)

        management.call_command("worker", "--until_done")

        self.assertQueue(0, task_name="div_zero", state=TaskExec.States.QUEUED)
        self.assertQueue(
            2, task_name="div_zero", state=TaskExec.States.FAILED, replaced=True
        )
        self.assertQueue(1, task_name="div_zero", state=TaskExec.States.SLEEPING)
        self.assertQueue(3)
        self.assertEqual(TaskExec.objects.last().retries, 8)
        self.assertEqual(TaskExec.objects.last().retry_delay, 40)

    @freeze_time("2020-01-01", as_kwarg="frozen_datetime")
    def test_task_due_date(self, frozen_datetime):
        """Checking unique task retries with delay"""

        @register_task(name="my_task", unique=True)
        def my_task(x):
            return x * 2

        def getDues():
            return [
                (t.state, f"{t.due:%Y-%m-%d %H:%M}")
                for t in TaskExec.objects.order_by("due")
            ]

        # Normal queue is due right now
        my_task.queue(1)
        self.assertEqual(
            getDues(),
            [
                (TaskExec.States.QUEUED.value, "2020-01-01 00:00"),
            ],
        )
        management.call_command("worker", "--until_done")
        self.assertEqual(
            getDues(),
            [
                (TaskExec.States.SUCCEEDED.value, "2020-01-01 00:00"),
            ],
        )

        # Delayed queue is due in the future
        my_task.queue(1, due=timezone.now() + datetime.timedelta(hours=3))
        self.assertEqual(
            getDues(),
            [
                (TaskExec.States.SUCCEEDED.value, "2020-01-01 00:00"),
                (TaskExec.States.SLEEPING.value, "2020-01-01 03:00"),
            ],
        )

        # Delayed closer reduces the due date
        my_task.queue(1, due=timezone.now() + datetime.timedelta(hours=2))
        self.assertEqual(
            getDues(),
            [
                (TaskExec.States.SUCCEEDED.value, "2020-01-01 00:00"),
                (TaskExec.States.SLEEPING.value, "2020-01-01 02:00"),
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

        self.assertTask(task_a, TaskExec.States.QUEUED)
        self.assertTask(task_b, TaskExec.States.QUEUED)
        self.assertTask(task_c, TaskExec.States.QUEUED)
        self.assertTask(task_d, TaskExec.States.QUEUED)
        self.assertTask(task_e, TaskExec.States.QUEUED)
        self.assertTask(task_f, TaskExec.States.QUEUED)
        self.assertTask(task_g, TaskExec.States.QUEUED)
        self.assertTask(task_h, TaskExec.States.QUEUED)

        # make sure tasks get assigned to default queue by default
        management.call_command("worker", "--until_done", "--queue", "default")

        self.assertTask(task_a, TaskExec.States.SUCCEEDED)
        self.assertTask(task_b, TaskExec.States.QUEUED)
        self.assertTask(task_c, TaskExec.States.QUEUED)
        self.assertTask(task_d, TaskExec.States.QUEUED)
        self.assertTask(task_e, TaskExec.States.QUEUED)
        self.assertTask(task_f, TaskExec.States.QUEUED)
        self.assertTask(task_g, TaskExec.States.QUEUED)
        self.assertTask(task_h, TaskExec.States.QUEUED)

        # make sure worker only runs their queue
        management.call_command("worker", "--until_done", "--queue", "queue_c")

        self.assertTask(task_a, TaskExec.States.SUCCEEDED)
        self.assertTask(task_b, TaskExec.States.QUEUED)
        self.assertTask(task_c, TaskExec.States.SUCCEEDED)
        self.assertTask(task_d, TaskExec.States.QUEUED)
        self.assertTask(task_e, TaskExec.States.QUEUED)
        self.assertTask(task_f, TaskExec.States.QUEUED)
        self.assertTask(task_g, TaskExec.States.QUEUED)
        self.assertTask(task_h, TaskExec.States.QUEUED)

        # make sure worker can run multiple queues
        management.call_command(
            "worker", "--until_done", "--queue", "queue_b", "--queue", "queue_d"
        )

        self.assertTask(task_a, TaskExec.States.SUCCEEDED)
        self.assertTask(task_b, TaskExec.States.SUCCEEDED)
        self.assertTask(task_c, TaskExec.States.SUCCEEDED)
        self.assertTask(task_d, TaskExec.States.SUCCEEDED)
        self.assertTask(task_e, TaskExec.States.QUEUED)
        self.assertTask(task_f, TaskExec.States.QUEUED)
        self.assertTask(task_g, TaskExec.States.QUEUED)
        self.assertTask(task_h, TaskExec.States.QUEUED)

        # make sure worker exclude queue works
        management.call_command(
            "worker",
            "--until_done",
            "--exclude_queue",
            "queue_g",
            "--exclude_queue",
            "queue_h",
        )

        self.assertTask(task_a, TaskExec.States.SUCCEEDED)
        self.assertTask(task_b, TaskExec.States.SUCCEEDED)
        self.assertTask(task_c, TaskExec.States.SUCCEEDED)
        self.assertTask(task_d, TaskExec.States.SUCCEEDED)
        self.assertTask(task_e, TaskExec.States.SUCCEEDED)
        self.assertTask(task_f, TaskExec.States.SUCCEEDED)
        self.assertTask(task_g, TaskExec.States.QUEUED)
        self.assertTask(task_h, TaskExec.States.QUEUED)

        # make sure worker run all queues by default
        management.call_command("worker", "--until_done")

        self.assertTask(task_a, TaskExec.States.SUCCEEDED)
        self.assertTask(task_b, TaskExec.States.SUCCEEDED)
        self.assertTask(task_c, TaskExec.States.SUCCEEDED)
        self.assertTask(task_d, TaskExec.States.SUCCEEDED)
        self.assertTask(task_e, TaskExec.States.SUCCEEDED)
        self.assertTask(task_f, TaskExec.States.SUCCEEDED)
        self.assertTask(task_g, TaskExec.States.SUCCEEDED)
        self.assertTask(task_h, TaskExec.States.SUCCEEDED)
