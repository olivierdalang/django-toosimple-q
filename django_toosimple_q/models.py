import contextlib
import io
import traceback
from datetime import timedelta

from django.db import models
from django.utils import timezone
from picklefield.fields import PickledObjectField

from .logging import logger
from .registry import schedules, tasks


class TaskExec(models.Model):
    """TaskExecution represent a specific planned or past call of a task, including inputs (arguments) and outputs.

    This is a model, whose instanced are typically created using `mycallable.queue()` or from schedules.
    """

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

    def execute(self):
        """Execute the task.

        A check is done to make sure the task is still queued.

        Returns True if at the task was executed, whether it failed or succeeded (so you can loop for testing).
        """

        self.refresh_from_db()
        if self.state != TaskExec.QUEUED and not (
            self.state == TaskExec.SLEEPING and timezone.now() >= self.due
        ):
            # this task was executed from another worker in the mean time
            return True

        if self.task_name not in tasks.keys():
            # this task is not in the registry
            self.state = TaskExec.INVALID
            self.save()
            logger.warning(f"{self} not found in registry [{list(tasks.keys())}]")
            return True

        task = tasks[self.task_name]

        logger.debug(f"Executing : {self}")

        self.started = timezone.now()
        self.state = TaskExec.PROCESSING
        self.save()

        try:
            stdout = io.StringIO()
            stderr = io.StringIO()

            try:
                with contextlib.redirect_stderr(stderr):
                    with contextlib.redirect_stdout(stdout):
                        self.result = task.callable(*self.args, **self.kwargs)
                self.state = TaskExec.SUCCEEDED
            except Exception:
                logger.warning(f"{self} failed !")
                self.state = TaskExec.FAILED
                self.result = traceback.format_exc()
                if self.retries != 0:
                    self.create_replacement(is_retry=True)
            finally:
                self.finished = timezone.now()
                self.stdout = stdout.getvalue()
                self.stderr = stderr.getvalue()
                self.save()

        except (KeyboardInterrupt, SystemExit) as e:
            logger.critical(f"{self} got interrupted !")
            self.state = TaskExec.INTERRUPTED
            self.create_replacement(is_retry=False)
            self.save()
            raise e

        return True

    def create_replacement(self, is_retry):
        if is_retry:
            retries = self.retries - 1 if self.retries > 0 else -1
            delay = self.retry_delay * 2
        else:
            retries = self.retries
            delay = self.retry_delay

        logger.info(f"Creating a replacement task for {self}")
        replaced_by = TaskExec.objects.create(
            task_name=self.task_name,
            args=self.args,
            kwargs=self.kwargs,
            priority=self.priority,
            created=self.created,
            retries=retries,
            retry_delay=delay,
            state=TaskExec.SLEEPING,
            due=timezone.now() + timedelta(seconds=self.retry_delay),
        )
        self.replaced_by = replaced_by
        self.save()


class ScheduleExec(models.Model):

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=1024, unique=True)
    last_check = models.DateTimeField(null=True, blank=True, default=timezone.now)
    last_run = models.ForeignKey(
        TaskExec, null=True, blank=True, on_delete=models.SET_NULL
    )

    def __str__(self):
        if self.name in schedules:
            return f"Schedule {self.name} [{schedules[self.name].cron}]"
        else:
            return f"Schedule {self.name} [invalid]"
