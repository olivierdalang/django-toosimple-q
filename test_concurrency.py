"""
This runs some concurrency tests. It sets up a database with simulated lag to
increase race conditions likelyhood, thus requires a running docker daemon.

Usage:
```
# Run these tests
python test_concurrency.py
```

WARNING: this will flush the database !!
"""

import os
import subprocess
import sys
import time
import unittest

import django
from django.db import OperationalError, connections
from joblib import Parallel, delayed

os.environ.setdefault("TOOSIMPLEQ_TEST_DB", "postgres")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "demoproject.settings")
django.setup()


def docker_call(cmd, check=True):
    sys_call(f"docker {cmd}", check)


def sys_call(cmd, check=True):
    try:
        process = subprocess.run(
            cmd, capture_output=True, encoding="utf-8", env=os.environ
        )
    except subprocess.CalledProcessError as e:
        if check:
            raise Exception(e.output)
        return e
    return process


COUNT = 32


class ConcurrencyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a postgres database
        docker_call("stop toxiproxy postgres", check=False)
        docker_call("stop toxiproxy postgres", check=False)
        docker_call(
            "run --name toxiproxy -p 5432:5432 -d --rm ghcr.io/shopify/toxiproxy"
        )
        docker_call(
            "run --name postgres -p 5433:5432 -e POSTGRES_PASSWORD=postgres -d --rm postgres"
        )
        docker_call(
            "exec toxiproxy /toxiproxy-cli create -l 0.0.0.0:5432 -u host.docker.internal:5433 postgres"
        )

        # Wait for connection
        cls.reconnect()

        # Migrate
        sys_call("python manage.py migrate")

    @classmethod
    def start_lag(cls):
        docker_call(
            "exec toxiproxy /toxiproxy-cli toxic add -t latency -n my_lag -a latency=100 -a jitter=5 postgres"
        )
        cls.reconnect()

    @classmethod
    def stop_lag(cls):
        docker_call("exec toxiproxy /toxiproxy-cli toxic del my_lag postgres")
        cls.reconnect()

    @classmethod
    def reconnect(cls):
        # disonnect any open connections
        for conn in connections.all():
            conn.close()
        # wait for postgres
        for retry in range(10):
            try:
                connections["default"].cursor()
                break
            except OperationalError:
                print(f"waiting for postgres (retry {retry})... ")
                time.sleep(1)

    def setUp(self):
        sys_call("python manage.py flush --noinput")

    def run_workers_batch(self, queue=None):
        self.start_lag()
        command = "python manage.py worker --until_done"
        if queue:
            command += f" --queue {queue}"
        outputs = Parallel(n_jobs=COUNT)(
            delayed(sys_call)(command) for i in range(COUNT)
        )
        self.stop_lag()

        # Ensure no worker failed
        errored_ouputs = [o for o in outputs if o.returncode != 0]
        err_lines = "\n".join([o.stderr.splitlines()[-1] for o in errored_ouputs])
        err_message = f"{len(errored_ouputs)} workers errored:\n{err_lines}"
        self.assertEqual(len(errored_ouputs), 0, err_message)

    def test_concurrency_schedules(self):
        from django_toosimple_q.models import ScheduleExec

        # Ensure no duplicate schedules were created
        self.assertEqual(ScheduleExec.objects.count(), 0)
        self.run_workers_batch(queue="concurrency_schedules")
        self.assertEqual(ScheduleExec.objects.count(), 1)

    def test_concurrency_tasks(self):
        from django.contrib.auth.models import User

        from django_toosimple_q.models import TaskExec

        TaskExec.objects.create(task_name="create_user", queue="concurrency_tasks")

        # Ensure the task really was just executed once
        self.assertEqual(TaskExec.objects.count(), 1)
        self.run_workers_batch(queue="concurrency_tasks")
        self.assertEqual(User.objects.count(), 1)


if __name__ == "__main__":
    if "--force" not in sys.argv:
        question = "This test will flush your database !\nAre you sure you want to continue ? (type 'yes' or run this with --force) "
        if input(question) != "yes":
            exit()
    else:
        sys.argv.remove("--force")
    unittest.main()
