import datetime
import random
import sys
import time

from django.utils import timezone
from django.utils.formats import time_format

from ...decorators import register_task, schedule_task
from ...models import TaskExec


@schedule_task(cron="* * * * * */30", datetime_kwarg="scheduled_time", queue="demo")
@register_task(name="say_hi", queue="demo")
def say_hi(scheduled_time):
    if scheduled_time is None:
        return "Hi ! This was not scheduled..."
    return f"Hi at {time_format(scheduled_time)}"


@schedule_task(cron="0 * * * * *", queue="demo")
@register_task(name="flaky", retries=3, retry_delay=2, queue="demo")
def flaky():
    if random.random() < 0.5:
        raise Exception("This failed, we'll retry !")
    else:
        return "This succeeded"


@schedule_task(cron="0 * * * * *", queue="demo")
@register_task(name="logging", queue="demo")
def logging():
    sys.stdout.write("This should go to standard output")
    sys.stderr.write("This should go to error output")
    return "This is the result"


@schedule_task(cron="0 */5 * * * *", queue="demo")
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


@schedule_task(cron="0 */30 * * * *", run_on_creation=True, queue="demo")
@register_task(name="cleanup", queue="demo", priority=-5)
def cleanup():
    old_tasks_execs = TaskExec.objects.filter(
        created__lte=timezone.now() - datetime.timedelta(minutes=10)
    )
    deletes = old_tasks_execs.delete()
    print(f"Deleted {deletes}")
    return True
