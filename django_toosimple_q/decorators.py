from django.core.exceptions import ImproperlyConfigured

from .registry import tasks, schedules


def register_task(name=None, queue='default', priority=0, unique=False):
    """Attaches ._task_name attribute, the .queue() method and adds the callable to the tasks registry"""

    def inner(func):
        if name:
            func._task_name = name
        else:
            func._task_name = func.__globals__["__name__"] + "." + func.__qualname__

        def enqueue(*args_, **kwargs_):
            from .models import Task
            if unique and Task.objects.filter(
                function=func._task_name,
                args=args_,
                kwargs=kwargs_,
                queue=queue,
                state=Task.QUEUED
            ).exists():
                return False
            return Task.objects.create(
                function=func._task_name,
                args=args_,
                kwargs=kwargs_,
                queue=queue,
                priority=priority
            )
        func.queue = enqueue
        tasks[func._task_name] = func
        return func

    return inner


def schedule(**kwargs):
    """Adds the task to the schedules registry"""

    def inner(func):
        if not hasattr(func, '_task_name'):
            raise ImproperlyConfigured(
                "Only registered tasks can be scheduled."
                " Are you sure you registered your callable with the @register_task() decorator ?"
            )
        schedule_name = kwargs.get('name', func._task_name)
        kwargs['name'] = schedule_name
        kwargs['function'] = func._task_name
        schedules[schedule_name] = kwargs
        return func

    return inner
