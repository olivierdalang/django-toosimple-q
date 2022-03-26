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
- django admin integration
- tasks results stored using the Django ORM

Limitations :

- no multithreading yet (but running multiple workers should work)
- not well suited for projects spawning a high volume of tasks

Compatibility:

- Django 3.2 and 4.0
- Python 3.8, 3.9, 3.10

## Installation

Install the library :
```shell
$ pip install django-toosimple-q
```

Enable the app in `settings.py` :
```python
INSTALLED_APPS = [
    ...
    'django_toosimple_q',
    ...
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
from django_toosimple_q.decorators import register_task, schedule

# Register and schedule tasks
@schedule(cron="30 8 * * *", args=['John'])
@register_task()
def morning_routine(name):
    return f"Good morning {name} !"
```

To consume the tasks, you need to run at least one worker :
```shell
$ python manage.py worker
```
The workers will take care of adding scheduled tasks to the queue when needed, and will execute the tasks.

The package autoloads `tasks.py` from all installed apps. While this is the recommended place to define your tasks, you can do so from anywhere in your code.

## Advanced usage

### Tasks

You can optionnaly give a custom name to your tasks. This is required when your task is defined in a local scope.
```python
@register_task(name="my_favourite_task")
def my_task(name):
    return f"Good morning {name} !"
```

You can set task priorities.
```python
@register_task(priority=0)
def my_favourite_task(name):
    return f"Good bye {name} !"

@register_task(priority=1)
def my_other_task(name):
    return f"Hello {name} !"

# Enqueue tasks
my_other_task.queue("John")
my_favourite_task.queue("Peter")  # will be executed before the other one
```

You can define `retries=N` and `retry_delay=S` to retry the task in case of failure. The delay (in second) will double on each failure.

```python
@register_task(retries=10, retry_delay=60)
def send_email():
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
def my_task(name):
    return f"Hello {name} !"

# Enqueue tasks
my_task.queue("John", due=timezone.now() + timedelta(hours=1))
```


You can get the task execution instance to access task exectution details. This can be
used with schedules as they set the due date properly.
```python
@register_task(taskexec_kwarg="taskexec")
def my_task(taskexec):
    return f"{taskexec} was supposed to run at {taskexec.due} and actully started at {taskexec.started}"
```

### Schedules

By default, `last_tick` is set to `now()` on schedule creation. This means they will only run on next cron occurence. If you need your schedules to be run as soon as possible after initialisation, you can specify `run_on_creation=True`.

```python
@schedule_task(cron="30 8 * * *", run_on_creation=True)
@register_task()
def my_task(name):
    return f"Good morning {name} !"
```

By default, if some crons where missed (e.g. after a server shutdown or if the workers can't keep up with all tasks), the missed tasks will be lost. If you need the tasks to catch up, set `catch_up=True`.

```python
@schedule_task(cron="30 8 * * *", catch_up=True)
@register_task()
def my_task(name):
    ...
```

You may define multiple schedules for the same task. In this case, it is mandatory to specify a unique name :

```python
@schedule_task(name="morning_routine", cron="30 16 * * *", args=['morning'])
@schedule_task(name="afternoon_routine", cron="30 8 * * *", args=['afternoon'])
@register_task()
def my_task(time):
    return f"Good {time} John !"
```

If you need the cron datetime inside the task, use the `due` field of the Task execution
instance, as described above :
```python
@schedule_task(cron="30 8 * * *")
@register_task(taskexec_kwarg="taskexec")
def my_task(taskexec):
    return f"This was scheduled for {taskexec.due.isoformat()}."
```

Similarly to tasks, you can assign schedules to specific queues, and then have your worker only consume tasks from specific queues using `--queue myqueue` or `--exclude_queue myqueue`.

```python

@register_schedule(queue='scheduler')
@register_task(queue='worker')
def task():
    ...

# Then run those with these workers
# manage.py worker --queue scheduler
# manage.py worker --queue worker
```

### Management comment

Besides standard django management commands arguments, the management command supports following arguments.

```
usage: manage.py worker [--queue QUEUE | --exclude_queue EXCLUDE_QUEUE]
                        [--tick TICK]
                        [--once | --until_done]

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
```

## Contrib apps

### django_toosimple_q.contrib.mail

A queued email backend to send emails asynchronously, preventing your website from failing completely in case the upstream backend is down.

#### Installation

Enable and configure the app in `settings.py` :
```python
INSTALLED_APPS = [
    ...
    'django_toosimple_q.contrib.mail',
    ...
]

EMAIL_BACKEND = 'django_toosimple_q.contrib.mail.backends.QueueBackend'

# Actual Django email backend used, defaults to django.core.mail.backends.smtp.EmailBackend, see https://docs.djangoproject.com/en/3.2/ref/settings/#email-backend
TOOSIMPLEQ_EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
```


## Dev

### Tests

To run tests locally (by default, tests runs against an in-memory sqlite database):

```shell
$ pip install -r requirements-dev.txt
$ python manage.py test
```

To run tests against postgres, run the following commands before :
```shell
# Start a local postgres database
$ docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres
# Set and env var
$ export TOOSIMPLEQ_TEST_DB=postgres # on Windows: `$Env:TOOSIMPLEQ_TEST_DB = "postgres"`
```

Tests are run automatically on github.

#### Manual testing

You can manually test the provided test project :
```shell
$ python manage.py migrate
$ python manage.py createsuperuser
$ python manage.py worker
$ python manage.py runserver
```

Then open http://127.0.0.1:8000/admin in your browser


### Contribute

Code style is done with pre-commit :
```
$ pip install -r requirements-dev.txt
$ pre-commit install
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

- 20xx-xx-xx : v1.0.0 **⚠ BACKWARDS INCOMPATIBLE RELEASE ⚠**
  - improved dealing with concurrency using locking (tested with 32 concurrent workers)
  - renamed `@schedule` -> `@schedule_task`
  - renamed models (`Schedule` -> `ScheduleExec` and `Task` -> `TaskExec`)
  - task name must now be provided as a kwarg (`@register_task("mytask")` -> `@register_task(name="mytask")`)
  - schedules are no longer stored in the database, only their execution infomation is (which means that `--recreate-only` and `--no-recreate` arguments are removed)
  - replaced last_check by run_on_creation argument in schedule_task decorator (`@schedule_task(..., last_chec=None)` -> `@schedule_task(..., run_on_creation=True)`)
  - included a demo project showcasing some custom tasks setups
  - updated compatibility to Django 3.2 and 4.0, and Python 3.8-3.10
  - added `due` argument to `task.queue()`
  - added `taskexec_kwarg` argument to `@register_task`, allowing to access the task execution instance from within the task
  - removed `datetime_kwarg` from `@register_schedule` (use `taskexec_kwarg` and the instance's `due` field instead)
  - added `queue` argument to `@register_schedule` (which allows pickup schedules selectively by worker)

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
