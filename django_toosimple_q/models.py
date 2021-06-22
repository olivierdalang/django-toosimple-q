import contextlib
import datetime
import io
import traceback

from croniter import croniter
from django.db import models
from django.utils import timezone
from picklefield.fields import PickledObjectField

from .logging import logger
from .registry import tasks


class Task(models.Model):
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

    function = models.CharField(max_length=1024)
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
        return f"Task {self.function} {self.icon}"

    @property
    def icon(self):
        if self.state == Task.SLEEPING:
            return "💤"
        elif self.state == Task.QUEUED:
            return "⌚"
        elif self.state == Task.PROCESSING:
            return "🚧"
        elif self.state == Task.SUCCEEDED:
            return "✔️"
        elif self.state == Task.FAILED:
            return "❌"
        elif self.state == Task.INTERRUPTED:
            return "🛑"
        elif self.state == Task.INVALID:
            return "⭕️"
        else:
            return "❓"

    def execute(self):
        """Execute the task.

        A check is done to make sure the task is still queued.

        Returns True if at the task was executed, whether it failed or succeeded (so you can loop for testing).
        """

        self.refresh_from_db()
        if self.state != Task.QUEUED and not (
            self.state == Task.SLEEPING and timezone.now() >= self.due
        ):
            # this task was executed from another worker in the mean time
            return True

        if self.function not in tasks.keys():
            # this task is not in the registry
            self.state = Task.INVALID
            self.save()
            logger.warning(f"{self} not found in registry [{list(tasks.keys())}]")
            return True

        logger.debug(f"Executing : {self}")

        self.started = timezone.now()
        self.state = Task.PROCESSING
        self.save()

        try:
            stdout = io.StringIO()
            stderr = io.StringIO()

            callable = tasks[self.function]

            # TODO : if callable is a string, load the callable using this pseudocode:
            # if is_string(callable):
            #     mod, call = self.function.rsplit(".", 1)
            #     callable = getattr(import_module(mod), call)

            try:
                with contextlib.redirect_stderr(stderr):
                    with contextlib.redirect_stdout(stdout):
                        self.result = callable(*self.args, **self.kwargs)
                self.state = Task.SUCCEEDED
            except Exception:
                logger.warning(f"{self} failed !")
                self.state = Task.FAILED
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
            self.state = Task.INTERRUPTED
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
        replaced_by = Task.objects.create(
            function=self.function,
            args=self.args,
            kwargs=self.kwargs,
            priority=self.priority,
            created=self.created,
            retries=retries,
            retry_delay=delay,
            state=Task.SLEEPING,
            due=timezone.now() + datetime.timedelta(seconds=self.retry_delay),
        )
        self.replaced_by = replaced_by
        self.save()


class Schedule(models.Model):

    name = models.CharField(max_length=1024, unique=True)
    function = models.CharField(max_length=1024)
    args = PickledObjectField(blank=True, default=list)
    kwargs = PickledObjectField(blank=True, default=dict)

    last_check = models.DateTimeField(null=True, default=timezone.now)
    catch_up = models.BooleanField(default=False)
    last_run = models.ForeignKey(Task, null=True, on_delete=models.SET_NULL)

    cron = models.CharField(max_length=1024)

    def execute_if_needed(self):
        """Execute the schedule if needed.

        A check is done to make sure the schedule wasn't checked by another worker in the mean time.

        The task may be added several times if catch_up is True.

        Returns True if at least one task was queued (so you can loop for testing).
        """

        last_check = self.last_check
        self.refresh_from_db()
        if last_check != self.last_check:
            # this schedule was executed from another worker in the mean time
            logger.info(f"Already executed : {self}")
            return True

        # we update last_check already to reduce race condition chance
        self.last_check = timezone.now()
        self.save()

        did_something = False
        next_due = croniter(self.cron, last_check or timezone.now()).get_next(
            datetime.datetime
        )
        while last_check is None or next_due <= timezone.now():

            logger.debug(f"Due : {self}")

            t = tasks[self.function].queue(*self.args, **self.kwargs)
            if t:
                self.last_run = t
                self.save()

            did_something = True

            if self.catch_up:
                last_check = next_due
            else:
                last_check = timezone.now()

            next_due = croniter(self.cron, last_check).get_next(datetime.datetime)

        return did_something

    def __str__(self):
        return f"Schedule {self.function} [{self.cron}]"
