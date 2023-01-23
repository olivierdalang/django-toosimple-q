# Django Too Simple Queue

[![PyPI version](https://badge.fury.io/py/django-toosimple-q.svg)](https://pypi.org/project/django-toosimple-q/) ![Workflow](https://github.com/olivierdalang/django-toosimple-q/workflows/ci/badge.svg)

This packages provides a simplistic task queue and scheduler for Django.

It is geared towards basic apps, where simplicity primes. The package offers simple decorator syntax, including cron-like schedules.

Features :

- no celery/redis/rabbitmq/whatever... just Django !
- clean decorator syntax to register tasks and schedules
- simple queuing syntax
- cron-like scheduling
- tasks.py autodiscovery
- supports autoreload
- django admin integration
- tasks results stored using the Django ORM
- replacement tasks on interruption

Limitations :

- no multithreading yet (but running multiple workers should work)
- not well suited for projects spawning a high volume of tasks

Compatibility:

- Django 3.2 and 4.0
- Python 3.8, 3.9, 3.10

## Installation

Install the library :
```shell
pip install django-toosimple-q
```

Enable the app in `settings.py` :
```python
INSTALLED_APPS = [
    # ...
    'django_toosimple_q',
    # ...
]
```

## Quickstart

Tasks need to be registered using the `@register_task()` decorator. Once registered, they can be added to the queue by calling the `.queue()` function.

```python
from django_toosimple_q.decorators import register_task

# Register a task
@register_task()
def my_task(name):
    return f"Hello {name} !"

# Enqueue tasks
my_task.queue("John")
my_task.queue("Peter")
```

Registered tasks can be scheduled from code using this cron-like syntax :
```python
from django_toosimple_q.decorators import register_task, schedule_task

# Register and schedule tasks (each morning at 8:30)
@schedule_task(cron="30 8 * * *", args=['John'])
@register_task()
def morning_routine(name):
    return f"Good morning {name} !"
```

To consume the tasks, you need to run at least one worker :
```shell
python manage.py worker
```
The workers will take care of adding scheduled tasks to the queue when needed, and will execute the tasks.

The package autoloads `tasks.py` from all installed apps. While this is the recommended place to define your tasks, you can do so from anywhere in your code.

## Advanced usage

### Tasks

You can optionnaly give a custom name to your tasks. This is required when your task is defined in a local scope.
```python
@register_task(name="my_favourite_task")
def my_task():
    ...
```

You can set task priorities.
```python
@register_task(priority=0)
def my_favourite_task():
    ...

@register_task(priority=1)
def my_other_task():
    ...

# Enqueue tasks
my_other_task.queue()
my_favourite_task.queue()  # will be executed before the other one
```

You can define `retries=N` and `retry_delay=S` to retry the task in case of failure. The delay (in second) will double on each failure.

```python
@register_task(retries=10, retry_delay=60)
def download_data():
    ...
```

You can mark a task as `unique=True` if the task shouldn't be queued again if already queued with the same arguments. This is usefull for tasks such as cleaning or refreshing.

```python
@register_task(unique=True)
def cleanup():
    ...

cleanup.queue()
cleanup.queue()  # this will be ignored as long as the first one is still queued
```

You can assign tasks to specific queues, and then have your worker only consume tasks from specific queues using `--queue myqueue` or `--exclude_queue myqueue`. By default, workers consume all tasks.

```python
@register_task(queue='long_running')
def long_task():
    ...

@register_task()
def short_task():
    ...

# Then run those with these workers, so that long
# running tasks don't prevent short running tasks
# from being run :
# manage.py worker --exclude_queue long_running
# manage.py worker
```

You can enqueue tasks with a specific due date.
```python
@register_task()
def my_task():
    ...

# Enqueue tasks
from datetime import datetime, timedelta
my_task.queue("John", due=datetime.now() + timedelta(hours=1))
```

The `queue()` function returns a `TaskExec` model instance, which holds information about the task execution, including the task result.

```python
from django.core.management import call_command
from django_toosimple_q.models import TaskExec

@register_task()
def multiply(a, b):
    return a * b

t = multiply.queue(3, 4)

assert t.state == TaskExec.States.QUEUED
assert t.result == None

call_command("worker", "--until_done")  # equivalent to `python manage.py worker --until_done`

t.refresh_from_db()
assert t.state == TaskExec.States.SUCCEEDED
assert t.result == 12
```

### Schedules

You may define multiple schedules for the same task. In this case, it is mandatory to specify a unique name :

```python
@schedule_task(name="afternoon_routine", cron="30 16 * * *", args=['afternoon'])
@schedule_task(name="morning_routine", cron="30 8 * * *", args=['morning'])
@register_task()
def my_task(time_of_day):
    return f"Good {time_of_day} John !"
```

By default, `last_run` is set to `now()` on schedule creation. This means they will only run on next cron occurence. If you need your schedules to be run as soon as possible after initialisation, you can specify `run_on_creation=True`.

```python
@schedule_task(cron="30 8 * * *", run_on_creation=True)
@register_task()
def my_task():
    ...
```

By default, if some crons where missed (e.g. after a server shutdown or if the workers can't keep up with all tasks), the missed tasks will be lost. If you need the tasks to catch up, set `catch_up=True`.

```python
@schedule_task(cron="30 8 * * *", catch_up=True)
@register_task()
def my_task():
    ...
```

You may get the schedule's cron datetime provided as a keyword argument to the task using the `datetime_kwarg` argument. This is often useful in combination with catch_up, for things like report generation. Remember to treat the case where the argument is `None` (which happens when the task is run outside of the schedule).

```python
@schedule_task(cron="30 8 * * *", datetime_kwarg="scheduled_on", catch_up=True)
@register_task()
def my_task(scheduled_on):
    if scheduled_on:
        return f"This was scheduled for {scheduled_on.isoformat()}."
    else:
        return "This was not scheduled."
```

Similarly to tasks, you can assign schedules to specific queues, and then have your worker only consume tasks from specific queues using `--queue myqueue` or `--exclude_queue myqueue`.

```python
@schedule_task(cron="30 8 * * *", queue='scheduler')
@register_task(queue='worker')
def task():
    ...

# Then run those with these workers
# manage.py worker --queue scheduler
# manage.py worker --queue worker
```

Schedule's cron support a non-standard sixth argument for seconds  :
```python
from django_toosimple_q.decorators import register_task, schedule_task

# A schedule running every 15 seconds
@schedule_task(cron="* * * * * */15")
@register_task()
def morning_routine():
    return f"15 seconds passed !"
```

### Management comment

Besides standard django management commands arguments, the management command supports following arguments.

```
usage: manage.py worker [--queue QUEUE | --exclude_queue EXCLUDE_QUEUE]
                        [--tick TICK]
                        [--once | --until_done]
                        [--label LABEL]
                        [--timeout TIMEOUT]
                        [--reload {always,never}]

optional arguments:
  --queue QUEUE         which queue to run (can be used several times, all
                        queues are run if not provided)
  --exclude_queue EXCLUDE_QUEUE
                        which queue not to run (can be used several times, all
                        queues are run if not provided)
  --tick TICK           frequency in seconds at which the database is checked
                        for new tasks/schedules
  --once                run once then exit (useful for debugging)
  --until_done          run until no tasks are available then exit (useful for
                        debugging)
  --label LABEL         the name of the worker to help identifying it ('{pid}'
                        will be replaced by the process id)
  --timeout TIMEOUT     the time in seconds after which this worker will be considered
                        offline (set this to a value higher than the longest tasks this
                        worker will execute)
  --reload {always,never}
                        watch for changes (by default, watches if DEBUG=True)
```

## Contrib apps

### django_toosimple_q.contrib.mail

A queued email backend to send emails asynchronously, preventing your website from failing completely in case the upstream backend is down.

Enable and configure the app in `settings.py` :
```python
INSTALLED_APPS = [
    # ...
    'django_toosimple_q.contrib.mail',
    # ...
]

EMAIL_BACKEND = 'django_toosimple_q.contrib.mail.backends.QueueBackend'

# Actual Django email backend used, defaults to django.core.mail.backends.smtp.EmailBackend, see https://docs.djangoproject.com/en/3.2/ref/settings/#email-backend
TOOSIMPLEQ_EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
```

Head to the [Django documentation](https://docs.djangoproject.com/en/4.0/topics/email/) for usage.

## Dev

### Automated tests

To run tests, we recommend using Docker :
```shell
docker compose build
# run all tests
docker compose run django test
# or to run just a specific test
docker compose run django test django_toosimple_q.tests.tests_worker.TestAutoreloadingWorker
```

Tests are run automatically on github.

### Manual testing

Manual testing can be done like this:

```shell
# start a dev server and a worker
docker compose build
docker compose run django migrate
docker compose run django createsuperuser
docker compose up
```

Then connect on 127.0.0.1:8000/admin/

### Without docker

To run tests locally without Docker (by default, tests runs against an in-memory sqlite database):

```shell
pip install -r requirements-dev.txt
python manage.py test
```

### Contribute

Code style is done with pre-commit :
```shell
pip install -r requirements-dev.txt
pre-commit install
```

## Internals

### Terms

**Task**: a callable with a known name in the *registry*. These are typically registered in `tasks.py`.

**TaskExecution**: a specific planned or past call of a *task*, including inputs (arguments) and outputs. This is a model, whose instanced are typically created using `mycallable.queue()` or from schedules.

**Schedule**: a configuration for repeated execution of *tasks*. These are typically configured in `tasks.py`.

**ScheduleExecution**: the last execution of a *schedule* (e.g. keeps track of the last time a schedule actually lead to generate a task execution).  This is a model, whose instances are created by the worker.

**Registry**: a dictionary keeping all registered schedules and tasks.

**Worker**: a management command that executes schedules and tasks on a regular basis.


## Changelog

- 2023-01-09 : v1.0.0b **⚠ BACKWARDS INCOMPATIBLE RELEASE ⚠**
  - known issues:
    - [ ] worker exit status not correctly set with autoreload
  - feature: added workerstatus to the admin, allowing to monitor workers
  - feature: queue tasks for later (`mytask.queue(due=now()+timedelta(hours=2))`)
  - feature: assign queues to schedules (`@schedule_task(queue="schedules")`)
  - feature: auto-reload when DEBUG is true
  - refactor: removed non-execution related data from the database (clarifying the fact tha the source of truth is the registry)
  - refactor: better support for concurrent workers
  - refactor: better names for models and decorators
  - infra: included a demo project
  - infra: improved testing, including for concurrency behaviour
  - infra: updated compatibility to Django 3.2/4.1 and Python 3.8-3.10
  - quick migration guide:
    - rename `@schedule` -> `@schedule_task`
    - task name must now be provided as a kwarg: `@register_task("mytask")` -> `@register_task(name="mytask")`)
    - replace `@schedule_task(..., last_check=None)` -> `@schedule_task(..., run_on_creation=True)`
    - models: `Schedule` -> `ScheduleExec` and `Task` -> `TaskExec`
    - renamed `ScheduleExec.last_run` to `ScheduleExec.last_task`

- 2022-03-24 : v0.4.0
  - made `last_check` and `last_run` optional in the admin
  - defined `id` fields

- 2021-07-15 : v0.3.0
  - added `contrib.mail`
  - task replacement now tracked with a FK instead of a state
  - also run tests on postgres
  - added `datetime_kwarg` argument to schedules

- 2021-06-11 : v0.2.0
  - added `retries`, `retry_delay` options for tasks
  - improve logging

- 2020-11-12 : v0.1.0
  - fixed bug where updating schedule failed
  - fixed worker not doing all available tasks for each tick
  - added --tick argument
  - enforce uniqueness of schedule
