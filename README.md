# Django Too Simple Queue

[![PyPI version](https://badge.fury.io/py/django-toosimple-q.svg)](https://pypi.org/project/django-toosimple-q/) ![Workflow](https://github.com/olivierdalang/django-toosimple-q/workflows/ci/badge.svg)

This packages provides a simplistic task queue and scheduler for Django.

If execution of your tasks is mission critical, do not use this library, and turn to more complex solutions such as Celery, as this package doesn't guarantee task execution nor unique execution.

It is geared towards basic apps, where simplicity primes over reliability. The package offers simple decorator syntax, including cron-like schedules.

Features :

- no celery/redis/rabbitmq/whatever... just Django !
- clean decorator syntax to register tasks and schedules
- simple queuing syntax
- cron-like scheduling
- tasks.py autodiscovery
- django admin integration

Limitations :

- probably not extremely reliable because of race conditions
- no multithreading yet (but running multiple workers should work)

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
@register_task("my_favourite_task")
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

### Schedules

By default, `last_run` is set to `now()` on schedule creation. This means they will only run on next cron occurence. If you need your schedules to be run as soon as possible after initialisation, you can specify `last_run=None`.

```python
@schedule(cron="30 8 * * *", last_run=None)
@register_task()
def my_task(name):
    return f"Good morning {name} !"
```

By default, if some crons where missed (e.g. after a server shutdown or if the workers can't keep up with all tasks), the missed tasks will be lost. If you need the tasks to catch up, set `catch_up=True`.

```python
@schedule(cron="30 8 * * *", catch_up=True)
@register_task()
def my_task(name):
    ...
```

You may define multiple schedules for the same task. In this case, it is mandatory to specify a unique name :

```python
@schedule(name="morning_routine", cron="30 16 * * *", args=['morning'])
@schedule(name="afternoon_routine", cron="30 8 * * *", args=['afternoon'])
@register_task()
def my_task(time):
    return f"Good {time} John !"
```

## Dev

### Tests

```shell
$ python manage.py test
```
