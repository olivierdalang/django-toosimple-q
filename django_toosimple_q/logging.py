import logging

formatter = logging.Formatter(
    fmt="[%(asctime)s %(levelname)s] [toosimpleq] %(message)s"
)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)

logger = logging.getLogger("toosimpleq")
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def show_registry():
    """Helper functions that shows the registry contents"""

    from .schedule import schedules_registry
    from .task import tasks_registry

    if len(schedules_registry):
        schedules_names = ", ".join(schedules_registry.keys())
        logger.info(
            f"Registry contains {len(schedules_registry)} schedules : {schedules_names}"
        )
    else:
        logger.info("Registry contains no schedules")

    if len(tasks_registry):
        tasks_names = ", ".join(tasks_registry.keys())
        logger.info(f"Registry contains {len(tasks_registry)} tasks : {tasks_names}")
    else:
        logger.info("Registry contains no schedules")
