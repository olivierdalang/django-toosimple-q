import contextlib
import io
import traceback
from datetime import timedelta
from typing import Callable

from django.db import transaction
from django.utils import timezone

from .logging import logger
from .models import TaskExec


class Task:
    """Represents an asnychronous task.

    This class is responsible of queuing and executing the tasks, by managing
    TaskExec instances."""

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

    def enqueue(self, *args_, due=None, **kwargs_):
        """Creates a TaskExec instance, effectively queuing execution of this task.

        Returns the created TaskExec, or False if no task was created (which can happen
        for tasks set as unique, if that task already exists)."""

        from .models import TaskExec

        logger.debug(f"Enqueuing task '{self.name}'")

        due_datetime = due or timezone.now()

        if self.unique:
            existing_tasks = TaskExec.objects.filter(
                task_name=self.name, args=args_, kwargs=kwargs_
            )
            # If already queued, we don't do anything
            queued_task = existing_tasks.filter(state=TaskExec.States.QUEUED).first()
            if queued_task is not None:
                return False
            # If there's already a same task that's sleeping
            sleeping_task = existing_tasks.filter(
                state=TaskExec.States.SLEEPING
            ).first()
            if sleeping_task is not None:
                if due is None:
                    # If the queuing is not delayed, we enqueue it now
                    sleeping_task.due = due_datetime
                    sleeping_task.state = TaskExec.States.QUEUED
                    sleeping_task.save()
                elif sleeping_task.due > due_datetime:
                    # If it's delayed to less than the current due date of the task
                    sleeping_task.due = min(sleeping_task.due, due_datetime)
                    sleeping_task.save()
                return False

        return TaskExec.objects.create(
            task_name=self.name,
            args=args_,
            kwargs=kwargs_,
            state=TaskExec.States.SLEEPING if due else TaskExec.States.QUEUED,
            due=due_datetime,
            retries=self.retries,
            retry_delay=self.retry_delay,
        )

    def execute(self, task_exec):
        """Execute the task.

        Returns True if at the task was executed, whether it failed or succeeded (so you can loop for testing).
        """

        logger.debug(f"Executing : {self}")

        try:
            with transaction.atomic():
                try:
                    stdout = io.StringIO()
                    stderr = io.StringIO()

                    with contextlib.redirect_stderr(stderr):
                        with contextlib.redirect_stdout(stdout):
                            task_exec.result = self.callable(
                                *task_exec.args, **task_exec.kwargs
                            )
                    task_exec.state = TaskExec.States.SUCCEEDED
                except Exception:
                    logger.warning(f"{task_exec} failed !")
                    task_exec.state = TaskExec.States.FAILED
                    task_exec.error = traceback.format_exc()
                    if task_exec.retries != 0:
                        self.create_replacement(task_exec, is_retry=True)
                finally:
                    task_exec.finished = timezone.now()
                    task_exec.stdout = stdout.getvalue()
                    task_exec.stderr = stderr.getvalue()
                    task_exec.save()
        except (KeyboardInterrupt, SystemExit) as e:
            logger.critical(f"{task_exec} got interrupted !")
            task_exec.state = TaskExec.States.INTERRUPTED
            task_exec.save()
            self.create_replacement(task_exec, is_retry=False)
            raise e

        return True

    def create_replacement(self, task_exec, is_retry):
        if is_retry:
            retries = task_exec.retries - 1 if task_exec.retries > 0 else -1
            delay = task_exec.retry_delay * 2
        else:
            retries = task_exec.retries
            delay = task_exec.retry_delay

        logger.info(f"Creating a replacement task for {task_exec}")
        replaced_by = TaskExec.objects.create(
            task_name=task_exec.task_name,
            args=task_exec.args,
            kwargs=task_exec.kwargs,
            retries=retries,
            retry_delay=delay,
            state=TaskExec.States.SLEEPING,
            due=timezone.now() + timedelta(seconds=task_exec.retry_delay),
        )
        task_exec.replaced_by = replaced_by
        task_exec.save()

    def __str__(self):
        return f"Task {self.name}"
