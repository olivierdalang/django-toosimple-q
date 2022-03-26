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
            else:
                return "❓"

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

    stdout = models.TextField(blank=True, default="")
    stderr = models.TextField(blank=True, default="")

    def __str__(self):
        return f"Task {self.task_name} {self.icon}"

    @property
    def priority(self):
        # TODO: use property for task instance instead
        try:
            return tasks_registry[self.task_name].priority
        except KeyError:
            return None

    @property
    def queue(self):
        # TODO: use property for task instance instead
        try:
            return tasks_registry[self.task_name].queue
        except KeyError:
            return None

    @property
    def unique(self):
        # TODO: use property for task instance instead
        try:
            return tasks_registry[self.task_name].unique
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
            else:
                return "❓"

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=1024, unique=True)
    last_tick = models.DateTimeField(default=timezone.now)
    last_run = models.ForeignKey(
        TaskExec, null=True, blank=True, on_delete=models.SET_NULL
    )
    state = models.CharField(
        max_length=32, choices=States.choices, default=States.ACTIVE
    )

    def __str__(self):
        return f"Task {self.name} {self.icon}"

    @property
    def cron(self):
        # TODO: use property for schedule instance instead
        try:
            return schedules_registry[self.name].cron
        except KeyError:
            return None

    @property
    def args(self):
        # TODO: use property for schedule instance instead
        try:
            return schedules_registry[self.name].args
        except KeyError:
            return None

    @property
    def kwargs(self):
        # TODO: use property for schedule instance instead
        try:
            return schedules_registry[self.name].kwargs
        except KeyError:
            return None

    @property
    def queue(self):
        # TODO: use property for schedule instance instead
        try:
            return schedules_registry[self.name].queue
        except KeyError:
            return None

    @property
    def priority(self):
        # TODO: use property for schedule instance instead
        try:
            return schedules_registry[self.name].priority
        except KeyError:
            return None

    @property
    def icon(self):
        return ScheduleExec.States.icon(self.state)
