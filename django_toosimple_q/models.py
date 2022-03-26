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
        QUEUED = "QUEUED", _("Queued")
        SLEEPING = "SLEEPING", _("Sleeping")
        PROCESSING = "PROCESSING", _("Processing")
        FAILED = "FAILED", _("Failed")
        SUCCEEDED = "SUCCEEDED", _("Succeeded")
        INVALID = "INVALID", _("Invalid")
        INTERRUPTED = "INTERRUPTED", _("Interrupted")

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
        try:
            return tasks_registry[self.task_name].priority
        except IndexError:
            return None

    @property
    def queue(self):
        try:
            return tasks_registry[self.task_name].queue
        except IndexError:
            return None

    @property
    def unique(self):
        try:
            return tasks_registry[self.task_name].unique
        except IndexError:
            return None

    @property
    def icon(self):
        if self.state == TaskExec.States.SLEEPING:
            return "💤"
        elif self.state == TaskExec.States.QUEUED:
            return "⌚"
        elif self.state == TaskExec.States.PROCESSING:
            return "🚧"
        elif self.state == TaskExec.States.SUCCEEDED:
            return "✔️"
        elif self.state == TaskExec.States.FAILED:
            return "❌"
        elif self.state == TaskExec.States.INTERRUPTED:
            return "🛑"
        elif self.state == TaskExec.States.INVALID:
            return "⚠️"
        else:
            return "❓"


class ScheduleExec(models.Model):
    class Meta:
        verbose_name = "Schedule Execution"

    class States(models.TextChoices):
        ACTIVE = "ACTIVE", _("Active")
        INVALID = "INVALID", _("Invalid")

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
        try:
            return schedules_registry[self.name].cron
        except IndexError:
            return None

    @property
    def queue(self):
        try:
            return schedules_registry[self.name].queue
        except IndexError:
            return None

    @property
    def priority(self):
        try:
            return schedules_registry[self.name].priority
        except IndexError:
            return None

    @property
    def icon(self):
        if self.state == ScheduleExec.States.ACTIVE:
            return "🟢"
        elif self.state == ScheduleExec.States.INVALID:
            return "⚠️"
        else:
            return "❓"
