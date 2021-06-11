from django.core.exceptions import ImproperlyConfigured
from django.utils import timezone

from .registry import schedules, tasks


def register_task(
    name=None, queue="default", priority=0, unique=False, retries=0, retry_delay=0
):
    """Attaches ._task_name attribute, the .queue() method and adds the callable to the tasks registry"""

    def inner(func):
        if name:
            func._task_name = name
        else:
            func._task_name = func.__globals__["__name__"] + "." + func.__qualname__

        def enqueue(*args_, **kwargs_):
            from .models import Task

            if unique:
                existing_tasks = Task.objects.filter(
                    function=func._task_name,
                    args=args_,
                    kwargs=kwargs_,
                    queue=queue,
                )
                # If already queued, we don't do anything
                queued_task = existing_tasks.filter(state=Task.QUEUED).first()
                if queued_task is not None:
                    return False
                # If there's a sleeping task, we set it's due date to now
                sleeping_task = existing_tasks.filter(state=Task.SLEEPING).first()
                if sleeping_task is not None:
                    sleeping_task.due = timezone.now()
                    sleeping_task.state = Task.QUEUED
                    sleeping_task.save()
                    return False

            return Task.objects.create(
                function=func._task_name,
                args=args_,
                kwargs=kwargs_,
                queue=queue,
                priority=priority,
                retries=retries,
                retry_delay=retry_delay,
            )

        func.queue = enqueue
        tasks[func._task_name] = func
        return func

    return inner


def schedule(**kwargs):
    """Adds the task to the schedules registry"""

    def inner(func):
        if not hasattr(func, "_task_name"):
            raise ImproperlyConfigured(
                "Only registered tasks can be scheduled."
                " Are you sure you registered your callable with the @register_task() decorator ?"
            )
        schedule_name = kwargs.get("name", func._task_name)
        kwargs["name"] = schedule_name
        kwargs["function"] = func._task_name
        schedules[schedule_name] = kwargs
        return func

    return inner
