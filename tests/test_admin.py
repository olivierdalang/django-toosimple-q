from django.contrib.auth.models import User
from django.test import Client, TestCase

from django_toosimple_q.decorators import register_task, schedule_task
from django_toosimple_q.models import ScheduleExec, TaskExec

from .utils import EmptyRegistryMixin, QueueAssertionMixin


class TestAdmin(QueueAssertionMixin, EmptyRegistryMixin, TestCase):
    def setUp(self):
        super().setUp()
        user = User.objects.create_superuser("admin", "test@example.com", "pass")

        self.client = Client()
        self.client.force_login(user)

        @schedule_task(cron="0 12 * * *", datetime_kwarg="scheduled_on")
        @register_task(name="a")
        def a(x):
            return x * 2

        self.task = TaskExec.objects.create(task_name="a")
        self.schedule = ScheduleExec.objects.create()

    def test_task_admin(self):
        """Check if task admin pages work"""

        response = self.client.get("/admin/toosimpleq/taskexec/")
        self.assertEqual(response.status_code, 200)

        response = self.client.get(f"/admin/toosimpleq/taskexec/{self.task.pk}/change/")
        self.assertEqual(response.status_code, 200)

    def test_schedule_admin(self):
        """Check if schedule admin pages work"""

        response = self.client.get("/admin/toosimpleq/scheduleexec/")
        self.assertEqual(response.status_code, 200)

        response = self.client.get(
            f"/admin/toosimpleq/scheduleexec/{self.schedule.pk}/change/"
        )
        self.assertEqual(response.status_code, 200)
