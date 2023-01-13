from datetime import datetime
from typing import Dict, List

from croniter import croniter, croniter_range
from django.db import transaction
from django.utils.timezone import now

from django_toosimple_q.models import ScheduleExec

from .logging import logger
from .registry import tasks_registry
from .task import Task


class Schedule:
    """A configuration for repeated execution of tasks. These are typically configured in `tasks.py`"""

    def __init__(
        self,
        name: str,
        task: Task,
        cron: str,
        queue: str = "default",
        args: List = [],
        kwargs: Dict = {},
        datetime_kwarg: str = None,
        catch_up: bool = False,
        run_on_creation: bool = False,
    ):
        self.name = name
        self.task = task
        self.cron = cron
        self.queue = queue
        self.args = args
        self.kwargs = kwargs
        self.datetime_kwarg = datetime_kwarg
        self.catch_up = catch_up
        self.run_on_creation = run_on_creation

    def execute(self, tick_duration=0, force=False):
        """Execute the schedule, which creates a new task if a new run is required
        since last check.

        The task may be added several times if catch_up is True.

        Schedules that have been checked less than tick_duration (in seconds) ago
        are ignored.

        Returns True if at least one task was queued (so you can loop for testing).
        """

        with transaction.atomic():
            # retrieve the last execution
            schedule_exec, created = ScheduleExec.objects.get_or_create(name=self.name)

            # By default, last_due is None on creation, meaning the schedule is due right away.
            # Optionally, it could be considered due only for the next cron.
            if created and not self.run_on_creation:
                schedule_exec.last_due = now()

            # Determine the next_dues
            if force:
                next_dues = [None]
            elif schedule_exec.last_due is None:
                # If the schedule has no last due date (probaby create with run_on_creation), we run it
                next_dues = [croniter(self.cron, now()).get_prev(datetime)]
            else:
                # Otherwise, we find all execution times since last check
                next_dues = list(
                    croniter_range(
                        schedule_exec.last_due, now(), self.cron, exclude_ends=True
                    )
                )
                # We keep only the last one if catchup wasn't specified
                if not self.catch_up:
                    next_dues = next_dues[-1:]

            # We enqueue the due tasks
            did_something = False
            for next_due in next_dues:

                logger.debug(f"{schedule_exec} is due at {next_due}")

                dt_kwarg = {}
                if self.datetime_kwarg:
                    dt_kwarg = {self.datetime_kwarg: next_due}

                t = tasks_registry[self.name].enqueue(
                    *self.args, due=next_due, **dt_kwarg, **self.kwargs
                )
                if t:
                    # TODO: enqueue returns None when the task is duplicated
                    schedule_exec.last_task = t

                # Update the last due
                schedule_exec.last_due = next_due
                did_something = True

            schedule_exec.state = ScheduleExec.States.ACTIVE
            schedule_exec.save()

        return did_something

    def __str__(self):
        return f"Schedule {self.name}"
