from django.contrib import admin
from django.contrib.admin.models import CHANGE, LogEntry
from django.contrib.auth.models import Permission, User
from django.contrib.contenttypes.models import ContentType
from django.core import management
from django.test import RequestFactory

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

    def test_manual_schedule_admin(self):
        """Check that manual schedule admin action work"""

        @schedule_task(cron="manual")
        @register_task(name="a")
        def a():
            return 2

        self.assertSchedule("a", None)
        management.call_command("worker", "--until_done")
        self.assertQueue(0)

        data = {
            "action": "action_force_run",
            "_selected_action": ScheduleExec.objects.get(name="a").pk,
        }
        response = self.client.post(
            "/admin/toosimpleq/scheduleexec/", data, follow=True
        )
        self.assertEqual(response.status_code, 200)

        self.assertQueue(1, state=TaskExec.States.QUEUED)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=TaskExec.States.SUCCEEDED)
        self.assertSchedule("a", ScheduleExec.States.ACTIVE)

    def test_schedule_admin_force_action(self):
        """Check if he force execute schedule action works"""

        @schedule_task(cron="13 0 1 1 *")
        @register_task(name="a")
        def a():
            return 2

        self.assertSchedule("a", None)
        self.assertQueue(0, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, state=TaskExec.States.QUEUED)

        management.call_command("worker", "--until_done")

        self.assertSchedule("a", ScheduleExec.States.ACTIVE)
        self.assertQueue(0, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, state=TaskExec.States.QUEUED)

        data = {
            "action": "action_force_run",
            "_selected_action": ScheduleExec.objects.get(name="a").pk,
        }
        response = self.client.post(
            "/admin/toosimpleq/scheduleexec/", data, follow=True
        )
        self.assertEqual(response.status_code, 200)

        management.call_command("worker", "--until_done")

        self.assertSchedule("a", ScheduleExec.States.ACTIVE)
        self.assertQueue(1, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(0, state=TaskExec.States.QUEUED)

        # ensure the action gets logged
        self.assertEqual(
            LogEntry.objects.filter(
                content_type_id=ContentType.objects.get_for_model(ScheduleExec).pk,
                action_flag=CHANGE,
            ).count(),
            1,
        )

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

        # ensure the action gets logged
        self.assertEqual(
            LogEntry.objects.filter(
                content_type_id=ContentType.objects.get_for_model(TaskExec).pk,
                action_flag=CHANGE,
            ).count(),
            1,
        )

    def test_task_admin_result_preview(self):
        """Check the the task results correctly displays, including if long"""

        @register_task()
        def a(length):
            return "o" * length

        # a short result appears as is
        a.queue(length=10)
        management.call_command("worker", "--until_done")
        response = self.client.get("/admin/toosimpleq/taskexec/", follow=True)
        self.assertContains(response, "o" * 10)

        # a long results gets trimmed
        a.queue(length=300)
        management.call_command("worker", "--until_done")
        response = self.client.get("/admin/toosimpleq/taskexec/", follow=True)
        self.assertContains(response, "o" * 254 + "…")

    def test_admin_actions_permissions(self):
        """Check that admin actions are only available with the correct permissions"""

        perms = Permission.objects.filter(
            codename__in=["force_run_scheduleexec", "requeue_taskexec"]
        )

        request_with = RequestFactory().get("/some-url/")
        request_with.user = User.objects.create(username="mike")
        request_with.user.user_permissions.set(perms)

        request_without = RequestFactory().get("/some-url/")
        request_without.user = User.objects.create(username="peter")

        # prefer admin.site.get_model_admin(TaskExec) once we drop support for 4.2
        task_model_admin = admin.site._registry[TaskExec]
        schedule_model_admin = admin.site._registry[ScheduleExec]

        self.assertCountEqual(
            task_model_admin.get_actions(request_with).keys(), ["action_requeue"]
        )
        self.assertCountEqual(
            schedule_model_admin.get_actions(request_with).keys(), ["action_force_run"]
        )
        self.assertCountEqual(task_model_admin.get_actions(request_without).keys(), [])
        self.assertCountEqual(
            schedule_model_admin.get_actions(request_without).keys(), []
        )
