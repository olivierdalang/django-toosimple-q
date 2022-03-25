from django.db.models import Count

from django_toosimple_q.models import TaskExec
from django_toosimple_q.schedule import schedules_registry
from django_toosimple_q.task import tasks_registry


class QueueAssertionMixin:
    """
    Adds assertQueue and assertTask helpers
    """

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

    def assertTask(self, task, expected_state):
        actual_state = TaskExec.objects.get(pk=task.pk).state
        if actual_state != expected_state:
            raise AssertionError(
                f"Expected {expected_state}, got {actual_state} [{task}]"
            )


class EmptyRegistryMixin:
    """
    Empties the schedules and task registries before running the test case (and restores it afterwards).

    Use this if you want to keep autoloaded task (from contrib.mail) from polluting the tests.
    """

    def setUp(self):
        self.__schedules_before = schedules_registry.copy()
        self.__tasks_before = tasks_registry.copy()
        schedules_registry.clear()
        tasks_registry.clear()

    def tearDown(self):
        schedules_registry.clear()
        tasks_registry.clear()
        schedules_registry.update(self.__schedules_before)
        tasks_registry.update(self.__tasks_before)
