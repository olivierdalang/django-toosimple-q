import time

from django.contrib.auth.models import User
from django.db import IntegrityError

from ...decorators import register_task, schedule_task


@schedule_task(cron="* * * * *", queue="schedules")
@register_task(name="create_user", queue="tasks")
def create_user():
    time.sleep(0.5)
    retry = 0
    while True:
        suffix = f"-copy{retry}" if retry > 0 else ""
        try:
            User.objects.create(username=f"user{suffix}")
            break
        except IntegrityError:
            retry += 1

    if retry > 0:
        raise Exception("Failed: had to rename the user")
    return 0


@register_task(name="sleep_task", queue="tasks")
def sleep_task(duration):
    time.sleep(duration)
    return True
