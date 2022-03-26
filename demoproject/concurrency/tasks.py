import time

from django.contrib.auth.models import User
from django.db import IntegrityError

from django_toosimple_q.decorators import register_task, schedule_task


@schedule_task(cron="* * * * *", queue="concurrency_schedules")
@register_task(name="create_user", taskexec_kwarg="taskexec", queue="concurrency_tasks")
def create_user(taskexec):
    time.sleep(0.5)
    retry = 0
    while True:
        suffix = f"-copy{retry}" if retry > 0 else ""
        try:
            User.objects.create(username=f"username-{taskexec.id}{suffix}")
            break
        except IntegrityError:
            retry += 1

    if retry > 0:
        raise Exception("Failed: had to rename the user")
    return 0
