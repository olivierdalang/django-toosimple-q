from django.core.exceptions import ImproperlyConfigured

from .registry import schedules_registry, tasks_registry
from .schedule import Schedule
from .task import Task


def register_task(**kwargs):
    """Attaches ._task attribute, the .queue() method and adds the callable to the tasks registry"""

    def inner(func):
        # Default name is the qualified function name
        if "name" not in kwargs:
            kwargs["name"] = func.__globals__["__name__"] + "." + func.__qualname__

        # Create the task instance
        kwargs["callable"] = func
        task = Task(**kwargs)

        # Attach that instance to the callable
        func._task = task

        # Include the `queue` callable
        func.queue = task.enqueue

        # Add to the registry
        tasks_registry[task.name] = task

        # Decorator returns the function itself
        return func

    return inner


def schedule_task(**kwargs):
    """Adds the task to the schedules registry"""

    def inner(func):
        if not hasattr(func, "_task"):
            raise ImproperlyConfigured(
                "Only registered tasks can be scheduled."
                " Are you sure you registered your callable with the @register_task() decorator ?"
            )

        # Default name is the name of the task
        if "name" not in kwargs:
            kwargs["name"] = func._task.name

        # Create the schedule instance
        kwargs["task"] = func._task
        schedule = Schedule(**kwargs)

        # Add to the registry
        schedules_registry[kwargs["name"]] = schedule

        # Decorator returns the function itself
        return func

    return inner
