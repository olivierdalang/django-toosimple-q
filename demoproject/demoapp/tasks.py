import datetime
import random
import sys
import time

from django.utils import timezone

from django_toosimple_q.decorators import register_task, schedule_task
from django_toosimple_q.models import TaskExec


@schedule_task(cron="* * * * *", datetime_kwarg="scheduled_time")
@register_task(name="say_hi")
def say_hi(scheduled_time):
    return f"Had to say hi {scheduled_time} (it is now {timezone.now()})"


@schedule_task(cron="* * * * *", queue="demo")
@register_task(name="flaky", retries=3, retry_delay=2, queue="demo")
def flaky():
    if random.random() < 0.5:
        raise Exception("This failed, we'll retry !")
    else:
        return "This succeeded"


@schedule_task(cron="* * * * *", queue="demo")
@register_task(name="logging", queue="demo")
def logging():
    sys.stdout.write("This should go to standard output")
    sys.stderr.write("This should go to error output")
    return "This is the result"


@schedule_task(cron="*/5 * * * *", queue="demo")
@register_task(name="long_running", queue="demo")
def long_running():
    text = f"started at {timezone.now()}\n"
    time.sleep(15)
    text += f"continue at {timezone.now()}\n"
    time.sleep(15)
    text += f"continue at {timezone.now()}\n"
    time.sleep(15)
    text += f"continue at {timezone.now()}\n"
    time.sleep(15)
    text += f"finishing at {timezone.now()}\n"
    return text


@schedule_task(cron="*/5 * * * *", run_on_creation=True, queue="demo-cleanup")
@register_task(name="cleanup", queue="demo-cleanup", priority=-5)
def cleanup():
    old_tasks_execs = TaskExec.objects.filter(
        created__lte=timezone.now() - datetime.timedelta(minutes=10)
    )
    deletes = old_tasks_execs.delete()
    print(f"Deleted {deletes}")
    return True
