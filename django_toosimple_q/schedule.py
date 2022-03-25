from datetime import datetime, timedelta
from typing import Dict, List

from croniter import croniter, croniter_range
from django.utils import timezone

from django_toosimple_q.models import ScheduleExec

from .logging import logger
from .task import Task, tasks_registry

schedules_registry: Dict[str, "Schedule"] = {}


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
        run_on_creation: bool = False,
    ):
        self.name = name
        self.task = task
        self.cron = cron
        self.args = args
        self.kwargs = kwargs
        self.datetime_kwarg = datetime_kwarg
        self.catch_up = catch_up
        self.run_on_creation = run_on_creation

    def execute(self, tick_duration):
        """Execute the schedule, which creates a new task if a new run is required
        since last check.

        The task may be added several times if catch_up is True.

        Schedules that have been checked less than tick_duration (in seconds) ago
        are ignored.

        Returns True if at least one task was queued (so you can loop for testing).
        """

        # retrieve the last execution
        execution, created = ScheduleExec.objects.get_or_create(name=self.name)

        last_check = execution.last_check

        # we update last_check already to reduce race condition chance
        execution.last_check = timezone.now()
        execution.status = ScheduleExec.States.ACTIVE
        execution.save()

        did_something = False

        if created and self.run_on_creation:
            # If the schedule is new, we run it now
            next_dues = [croniter(self.cron, timezone.now()).get_prev(datetime)]
        elif timezone.now() - last_check < timedelta(seconds=tick_duration):
            # If the last check was less than a tick ago (usually only happens when testing with until_done)
            next_dues = []
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

            t = tasks_registry[self.name].enqueue(*self.args, **self.kwargs, **dt_kwarg)
            if t:
                execution.last_run = t
                execution.save()

            did_something = True

        return did_something
