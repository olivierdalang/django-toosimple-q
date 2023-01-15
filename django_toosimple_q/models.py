import datetime
from typing import List

from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from picklefield.fields import PickledObjectField

from .registry import schedules_registry, tasks_registry


class TaskExec(models.Model):
    """TaskExecution represent a specific planned or past call of a task, including inputs (arguments) and outputs.

    This is a model, whose instanced are typically created using `mycallable.queue()` or from schedules.
    """

    class Meta:
        verbose_name = "Task Execution"

    class States(models.TextChoices):
        SLEEPING = "SLEEPING", _("Sleeping")
        QUEUED = "QUEUED", _("Queued")
        PROCESSING = "PROCESSING", _("Processing")
        SUCCEEDED = "SUCCEEDED", _("Succeeded")
        INTERRUPTED = "INTERRUPTED", _("Interrupted")
        FAILED = "FAILED", _("Failed")
        INVALID = "INVALID", _("Invalid")

        @classmethod
        def icon(cls, state):
            if state == cls.SLEEPING:
                return "💤"
            elif state == cls.QUEUED:
                return "⌚"
            elif state == cls.PROCESSING:
                return "🚧"
            elif state == cls.SUCCEEDED:
                return "✔️"
            elif state == cls.FAILED:
                return "❌"
            elif state == cls.INTERRUPTED:
                return "🛑"
            elif state == cls.INVALID:
                return "⚠️"
            raise NotImplementedError(f"Unknown state: {state}")

        @classmethod
        def todo(cls) -> List[str]:
            """A list of values that are not done (opposite of done)"""
            return [
                cls.SLEEPING.value,
                cls.QUEUED.value,
                cls.PROCESSING.value,
            ]

        @classmethod
        def done(cls) -> List[str]:
            """A list of values that are done (opposite of todo)"""
            return [v for v in cls.values if v not in cls.todo()]

    id = models.BigAutoField(primary_key=True)
    task_name = models.CharField(max_length=1024)
    args = PickledObjectField(blank=True, default=list)
    kwargs = PickledObjectField(blank=True, default=dict)
    retries = models.IntegerField(
        default=0, help_text="retries left, -1 means infinite"
    )
    retry_delay = models.IntegerField(
        default=0,
        help_text="Delay before next retry in seconds. Will double after each failure.",
    )

    due = models.DateTimeField(default=timezone.now)
    created = models.DateTimeField(default=timezone.now)
    started = models.DateTimeField(blank=True, null=True)
    finished = models.DateTimeField(blank=True, null=True)
    state = models.CharField(
        max_length=32, choices=States.choices, default=States.QUEUED
    )
    result = PickledObjectField(blank=True, null=True)
    error = models.TextField(blank=True, null=True)
    replaced_by = models.ForeignKey(
        "self", null=True, blank=True, on_delete=models.SET_NULL
    )
    worker = models.ForeignKey(
        "WorkerStatus", null=True, blank=True, on_delete=models.SET_NULL
    )

    stdout = models.TextField(blank=True, default="")
    stderr = models.TextField(blank=True, default="")

    def __str__(self):
        return f"Task '{self.task_name}' {self.icon} [{self.id}]"

    @property
    def task(self):
        """The corresponding task instance, or None if it's not in the registry"""
        try:
            return tasks_registry[self.task_name]
        except KeyError:
            return None

    @property
    def icon(self):
        return TaskExec.States.icon(self.state)


class ScheduleExec(models.Model):
    class Meta:
        verbose_name = "Schedule Execution"

    class States(models.TextChoices):
        ACTIVE = "ACTIVE", _("Active")
        INVALID = "INVALID", _("Invalid")

        @classmethod
        def icon(cls, state):
            if state == cls.ACTIVE:
                return "🟢"
            elif state == cls.INVALID:
                return "⚠️"
            raise NotImplementedError(f"Unknown state: {state}")

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=1024, unique=True)
    last_due = models.DateTimeField(null=True, blank=True)
    last_task = models.ForeignKey(
        TaskExec, null=True, blank=True, on_delete=models.SET_NULL
    )
    state = models.CharField(
        max_length=32, choices=States.choices, default=States.ACTIVE
    )

    def __str__(self):
        return f"Schedule '{self.name}' {self.icon}"

    @property
    def schedule(self):
        """The corresponding schedule instance, or None if it's not in the registry"""
        try:
            return schedules_registry[self.name]
        except KeyError:
            return None

    @property
    def icon(self):
        return ScheduleExec.States.icon(self.state)


class WorkerStatus(models.Model):
    """Represents the status of a worker. At each tick, the worker will update it's status.
    After a certain tim"""

    class Meta:
        verbose_name = "Worker Status"
        verbose_name_plural = "Workers Statuses"

    class ExitCodes(models.IntegerChoices):
        STOPPED = 0, _("Stopped")
        TERMINATED = 77, _("Terminated")
        CRASHED = 99, _("Crashed")

    class States(models.TextChoices):
        ONLINE = "ONLINE", _("Online")
        STOPPED = "STOPPED", _("Stopped")
        TERMINATED = "TERMINATED", _("Terminated")
        CRASHED = "CRASHED", _("Crashed")
        TIMEDOUT = "TIMEDOUT", _("Timedout")

        @classmethod
        def icon(cls, state):
            if state == cls.ONLINE:
                return "🟢"
            elif state == cls.STOPPED:
                return "⚪"
            elif state == cls.TERMINATED:
                return "🟧"
            elif state == cls.CRASHED:
                return "🟥"
            elif state == cls.TIMEDOUT:
                return "❓"
            raise NotImplementedError(f"Unknown state: {state}")

    id = models.BigAutoField(primary_key=True)
    label = models.CharField(max_length=1024, unique=True)
    included_queues = models.JSONField(default=list)
    excluded_queues = models.JSONField(default=list)
    timeout = models.DurationField(default=datetime.timedelta(hours=1))
    last_tick = models.DateTimeField(default=timezone.now)
    started = models.DateTimeField(default=timezone.now)
    stopped = models.DateTimeField(null=True, blank=True)
    exit_code = models.IntegerField(choices=ExitCodes.choices, null=True, blank=True)
    exit_log = models.TextField(null=True, blank=True)

    @property
    def state(self):
        if self.stopped:
            if self.exit_code == WorkerStatus.ExitCodes.STOPPED:
                return WorkerStatus.States.STOPPED
            elif self.exit_code == WorkerStatus.ExitCodes.TERMINATED:
                return WorkerStatus.States.TERMINATED
            else:
                return WorkerStatus.States.CRASHED
        elif self.last_tick < timezone.now() - self.timeout:
            return WorkerStatus.States.TIMEDOUT
        else:
            return WorkerStatus.States.ONLINE

    def __str__(self):
        return f"Worker '{self.label}' {self.icon}"

    @property
    def icon(self):
        return WorkerStatus.States.icon(self.state)
