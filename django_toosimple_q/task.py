from typing import Callable

from django.utils import timezone

from .logging import logger


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

    def __str__(self):
        return f"Task {self.name}"
