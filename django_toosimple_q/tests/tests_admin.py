from django.core import management

from django_toosimple_q.decorators import register_task, schedule_task
from django_toosimple_q.models import ScheduleExec, TaskExec

from .base import TooSimpleQRegularTestCase


class TestAdmin(TooSimpleQRegularTestCase):
    def test_task_admin(self):
        """Check if task admin pages work"""

        @register_task(name="a")
        def a():
            return 2

        task_exec = a.queue()

        management.call_command("worker", "--until_done")

        response = self.client.get("/admin/toosimpleq/taskexec/")
        self.assertEqual(response.status_code, 200)

        response = self.client.get(f"/admin/toosimpleq/taskexec/{task_exec.pk}/change/")
        self.assertEqual(response.status_code, 200)

    def test_schedule_admin(self):
        """Check if schedule admin pages work"""

        @schedule_task(cron="* * * * *")
        @register_task(name="a")
        def a():
            return 2

        management.call_command("worker", "--until_done")

        response = self.client.get("/admin/toosimpleq/scheduleexec/")
        self.assertEqual(response.status_code, 200)

        scheduleexec = ScheduleExec.objects.first()
        response = self.client.get(
            f"/admin/toosimpleq/scheduleexec/{scheduleexec.pk}/change/"
        )
        self.assertEqual(response.status_code, 200)

    def test_task_admin_requeue_action(self):
        """Check if the requeue action works"""

        @register_task(name="a")
        def a():
            return 2

        task_exec = a.queue()

        self.assertQueue(0, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, state=TaskExec.States.QUEUED)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, state=TaskExec.States.QUEUED)

        data = {
            "action": "action_requeue",
            "_selected_action": task_exec.pk,
        }
        response = self.client.post("/admin/toosimpleq/taskexec/", data, follow=True)
        self.assertEqual(response.status_code, 200)

        self.assertQueue(1, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1, state=TaskExec.States.QUEUED)

        management.call_command("worker", "--until_done")

        self.assertQueue(2, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, state=TaskExec.States.QUEUED)
