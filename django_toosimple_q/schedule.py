from datetime import datetime
from typing import Dict, List, Optional

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

    def execute(self, dues: List[Optional[datetime]]):
        """Enqueues the related tasks at the given due dates"""

        # We enqueue the due tasks
        for due in dues:
            logger.debug(f"{self} is due at {due}")

            dt_kwarg = {}
            if self.datetime_kwarg:
                dt_kwarg = {self.datetime_kwarg: due}

            tasks_registry[self.name].enqueue(
                *self.args, due=due, **dt_kwarg, **self.kwargs
            )

    def __str__(self):
        return f"Schedule {self.name}"
