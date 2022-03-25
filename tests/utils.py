from django.contrib.auth.models import User
from django.core import mail
from django.db.models import Count
from django.test import Client, TestCase

from django_toosimple_q.models import TaskExec
from django_toosimple_q.schedule import schedules_registry
from django_toosimple_q.task import tasks_registry


class TooSimpleQTestCase(TestCase):
    """
    Base TestCase for TooSimpleQ.

    - Clears the schedules and task registries (reverting the autodiscovery)
    - Creates a superuser
    - Clears the mailbox
    - Adds assertQueue and assertTask helpers

    Use this if you want to keep autoloaded task (from contrib.mail) from polluting the tests.
    """

    def setUp(self):
        # Clean the registry
        schedules_registry.clear()
        tasks_registry.clear()

        # Create a superuser
        user = User.objects.create_superuser("admin", "test@example.com", "pass")
        self.client = Client()
        self.client.force_login(user)

        # Clear the mailbox
        mail.outbox.clear()

    def assertQueue(self, expected_count, task_name=None, state=None, replaced=None):
        tasks = TaskExec.objects.all()
        if task_name:
            tasks = tasks.filter(task_name=task_name)
        if state:
            tasks = tasks.filter(state=state)
        if replaced is not None:
            tasks = tasks.filter(replaced_by__isnull=not replaced)
        actual_count = tasks.count()
        if actual_count != expected_count:
            vals = (
                TaskExec.objects.values("task_name", "state")
                .annotate(count=Count("*"))
                .order_by("task_name", "state")
            )
            debug = "\n".join(
                f"{v['task_name']}/{v['state']} : {v['count']}" for v in vals
            )
            raise AssertionError(
                f"Expected {expected_count} tasks, got {actual_count} tasks.\n{debug}"
            )

    def assertResults(self, expected=[], task_name=None):
        tasks_execs = TaskExec.objects.order_by("created")
        if task_name:
            tasks_execs = tasks_execs.filter(task_name=task_name)
        results = list(tasks_execs.values_list("result", flat=True))

        self.assertEqual(results, expected)

    def assertTask(self, task, expected_state):
        actual_state = TaskExec.objects.get(pk=task.pk).state
        if actual_state != expected_state:
            raise AssertionError(
                f"Expected {expected_state}, got {actual_state} [{task}]"
            )
