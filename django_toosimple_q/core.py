from datetime import datetime
from typing import Callable, Dict, List

from croniter import croniter, croniter_range
from django.utils import timezone

from django_toosimple_q.models import ScheduleExec

from .logging import logger
from .registry import tasks


class Task:
    def __init__(
        self,
        name: str,
        callable: Callable,
        queue: str = "default",
        priority: int = 0,
        unique: bool = False,
        retries: int = 0,
        retry_delay: int = 0,
    ):
        self.name = name
        self.callable = callable
        self.queue = queue
        self.priority = priority
        self.unique = unique
        self.retries = retries
        self.retry_delay = retry_delay

    def enqueue(self, *args_, **kwargs_):
        from .models import TaskExec

        if self.unique:
            existing_tasks = TaskExec.objects.filter(
                function=self.name, args=args_, kwargs=kwargs_, queue=self.queue
            )
            # If already queued, we don't do anything
            queued_task = existing_tasks.filter(state=TaskExec.QUEUED).first()
            if queued_task is not None:
                return False
            # If there's a sleeping task, we set it's due date to now
            sleeping_task = existing_tasks.filter(state=TaskExec.SLEEPING).first()
            if sleeping_task is not None:
                sleeping_task.due = timezone.now()
                sleeping_task.state = TaskExec.QUEUED
                sleeping_task.save()
                return False

        return TaskExec.objects.create(
            function=self.name,
            args=args_,
            kwargs=kwargs_,
            queue=self.queue,
            priority=self.priority,
            retries=self.retries,
            retry_delay=self.retry_delay,
        )


class Schedule:
    """A configuration for repeated execution of tasks. These are typically configured in `tasks.py`"""

    def __init__(
        self,
        name: str,
        task: Task,
        cron: str,
        args: List = [],
        kwargs: Dict = {},
        datetime_kwarg: str = None,
        catch_up: bool = False,
        last_check=timezone.now,  # TODO: replace by run_on_creation or default_last_check
    ):
        self.name = name
        self.task = task
        self.cron = cron
        self.args = args
        self.kwargs = kwargs
        self.datetime_kwarg = datetime_kwarg
        self.catch_up = catch_up
        self.last_check = last_check

    def execute(self):
        """Execute the schedule, which creates a new task if a new run is required
        since last check.

        The task may be added several times if catch_up is True.

        Returns True if at least one task was queued (so you can loop for testing).
        """

        # retrieve the last execution
        execution, created = ScheduleExec.objects.get_or_create(
            name=self.name, defaults={"last_check": self.last_check}
        )

        last_check = execution.last_check

        # we update last_check already to reduce race condition chance
        execution.last_check = timezone.now()
        execution.save()

        did_something = False

        if last_check is None:
            # If the schedule was never checked, we run it now
            next_dues = [croniter(self.cron, timezone.now()).get_prev(datetime)]
        else:
            # Otherwise, we find all execution times since last check
            next_dues = list(croniter_range(last_check, timezone.now(), self.cron))
            # We keep only the last one if catchup wasn't specified
            if not self.catch_up:
                next_dues = next_dues[-1:]

        for next_due in next_dues:

            logger.debug(f"Due : {self}")

            dt_kwarg = {}
            if self.datetime_kwarg:
                dt_kwarg = {self.datetime_kwarg: next_due}

            t = tasks[self.name].enqueue(*self.args, **self.kwargs, **dt_kwarg)
            if t:
                execution.last_run = t
                execution.save()

            did_something = True

        return did_something
