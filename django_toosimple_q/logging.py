import logging

from .registry import schedules_registry, tasks_registry

logger = logging.getLogger("toosimpleq")


def show_registry():
    """Helper functions that shows the registry contents"""

    if len(schedules_registry):
        schedules_names = ", ".join(schedules_registry.keys())
        logger.info(
            f"{len(schedules_registry)} schedules registered: {schedules_names}"
        )
    else:
        logger.info("No schedules registered")

    if len(tasks_registry):
        tasks_names = ", ".join(tasks_registry.keys())
        logger.info(f"{len(tasks_registry)} tasks registered: {tasks_names}")
    else:
        logger.info("No tasks registered")
