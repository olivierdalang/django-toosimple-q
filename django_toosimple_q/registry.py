from .logging import logger

tasks = {}
schedules = {}


def dump_registry():
    """
    Logs the state of the registry for debugging purposes
    """

    if len(schedules):
        schedules_names = ", ".join(schedules.keys())
        logger.info(f"Registry contains {len(schedules)} schedules : {schedules_names}")
    else:
        logger.info("Registry contains no schedules")

    if len(tasks):
        tasks_names = ", ".join(tasks.keys())
        logger.info(f"Registry contains {len(tasks)} tasks : {tasks_names}")
    else:
        logger.info("Registry contains no schedules")
