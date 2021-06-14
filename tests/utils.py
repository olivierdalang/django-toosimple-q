from django.db.models import Count

from django_toosimple_q.models import Task


class QueueAssertionMixin:
    def assertQueue(self, expected_count, function=None, state=None, replaced=None):
        tasks = Task.objects.all()
        if function:
            tasks = tasks.filter(function=function)
        if state:
            tasks = tasks.filter(state=state)
        if replaced is not None:
            tasks = tasks.filter(replaced_by__isnull=not replaced)
        actual_count = tasks.count()
        if actual_count != expected_count:
            vals = (
                Task.objects.values("function", "state")
                .annotate(count=Count("*"))
                .order_by("function", "state")
            )
            debug = "\n".join(
                f"{v['function']}/{v['state']} : {v['count']}" for v in vals
            )
            raise AssertionError(
                f"Expected {expected_count} tasks, got {actual_count} tasks.\n{debug}"
            )

    def assertTask(self, task, expected_state):
        actual_state = Task.objects.get(pk=task.pk).state
        if actual_state != expected_state:
            raise AssertionError(
                f"Expected {expected_state}, got {actual_state} [{task}]"
            )
