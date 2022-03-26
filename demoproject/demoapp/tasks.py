import datetime
import random
import sys

from django.utils import timezone

from django_toosimple_q.decorators import register_task, schedule_task
from django_toosimple_q.models import TaskExec


@schedule_task(cron="* * * * *")
@register_task(name="say_hi", taskexec_kwarg="taskexec")
def say_hi(taskexec):
    return f"Had to say hi {taskexec.due} (it is now {timezone.now()})"


@schedule_task(cron="* * * * *")
@register_task(name="flaky", retries=3, retry_delay=2)
def flaky():
    if random.random() < 0.5:
        raise Exception("This failed, we'll retry !")
    else:
        return "This succeeded"


@schedule_task(cron="* * * * *")
@register_task(name="logging")
def logging():
    sys.stdout.write("This should go to standard output")
    sys.stderr.write("This should go to error output")
    return "This is the result"


@schedule_task(cron="* * * * *")
@register_task(name="task_instance", taskexec_kwarg="taskexec")
def task_instance(taskexec):
    return f"{taskexec} was supposed to run at {taskexec.due} and actully started at {taskexec.started}"


@schedule_task(cron="*/5 * * * *", run_on_creation=True)
@register_task(name="cleanup")
def cleanup():
    old_tasks_execs = TaskExec.objects.filter(
        created__lte=timezone.now() - datetime.timedelta(minutes=10)
    )
    deletes = old_tasks_execs.delete()
    print(f"Deleted {deletes}")
    return True
