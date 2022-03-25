from django.db import models
from django.utils import timezone
from picklefield.fields import PickledObjectField


class TaskExec(models.Model):
    """TaskExecution represent a specific planned or past call of a task, including inputs (arguments) and outputs.

    This is a model, whose instanced are typically created using `mycallable.queue()` or from schedules.
    """

    class Meta:
        verbose_name = "Task Execution"

    QUEUED = "QUEUED"
    SLEEPING = "SLEEPING"
    PROCESSING = "PROCESSING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"
    INVALID = "INVALID"
    INTERRUPTED = "INTERRUPTED"

    state_choices = (
        (QUEUED, "QUEUED"),
        (SLEEPING, "SLEEPING"),
        (PROCESSING, "PROCESSING"),
        (FAILED, "FAILED"),
        (SUCCEEDED, "SUCCEEDED"),
        (INVALID, "INVALID"),
        (INTERRUPTED, "INTERRUPTED"),
    )

    id = models.BigAutoField(primary_key=True)
    task_name = models.CharField(max_length=1024)
    args = PickledObjectField(blank=True, default=list)
    kwargs = PickledObjectField(blank=True, default=dict)
    queue = models.CharField(max_length=32, default="default")
    priority = models.IntegerField(default=0)
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
    state = models.CharField(max_length=32, choices=state_choices, default=QUEUED)
    result = PickledObjectField(blank=True, null=True)
    replaced_by = models.ForeignKey(
        "self", null=True, blank=True, on_delete=models.SET_NULL
    )

    stdout = models.TextField(blank=True, default="")
    stderr = models.TextField(blank=True, default="")

    def __str__(self):
        return f"Task {self.task_name} {self.icon}"

    @property
    def icon(self):
        if self.state == TaskExec.SLEEPING:
            return "💤"
        elif self.state == TaskExec.QUEUED:
            return "⌚"
        elif self.state == TaskExec.PROCESSING:
            return "🚧"
        elif self.state == TaskExec.SUCCEEDED:
            return "✔️"
        elif self.state == TaskExec.FAILED:
            return "❌"
        elif self.state == TaskExec.INTERRUPTED:
            return "🛑"
        elif self.state == TaskExec.INVALID:
            return "⭕️"
        else:
            return "❓"


class ScheduleExec(models.Model):
    class Meta:
        verbose_name = "Schedule Execution"

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=1024, unique=True)
    last_check = models.DateTimeField(null=True, blank=True, default=timezone.now)
    last_run = models.ForeignKey(
        TaskExec, null=True, blank=True, on_delete=models.SET_NULL
    )

    def __str__(self):
        from .schedule import schedules_registry

        if self.name in schedules_registry:
            return f"Schedule {self.name} [{schedules_registry[self.name].cron}]"
        else:
            return f"Schedule {self.name} [invalid]"
