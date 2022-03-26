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
import unittest

from django.contrib.auth.models import User
from django.test import TransactionTestCase
from joblib import Parallel, delayed

from django_toosimple_q.models import ScheduleExec, TaskExec

from .concurrency.utils import prepare_toxiproxy, sys_call

COUNT = 32


@unittest.skipIf(
    os.environ.get("TOOSIMPLEQ_TEST_DB") != "postgres", "requires postgres backend"
)
class ConcurrencyTest(TransactionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        prepare_toxiproxy()

    def run_workers_batch(self, queue=None):
        command = "python manage.py worker --until_done"
        if queue:
            command += f" --queue {queue}"

        outputs = Parallel(n_jobs=COUNT)(
            delayed(sys_call)(command) for i in range(COUNT)
        )

        # Ensure no worker failed
        errored_ouputs = [o for o in outputs if o.returncode != 0]
        err_lines = "\n".join([o.stderr.splitlines()[-1] for o in errored_ouputs])
        err_message = f"{len(errored_ouputs)} workers errored:\n{err_lines}"
        self.assertEqual(len(errored_ouputs), 0, err_message)

    def test_schedules(self):

        # Ensure no duplicate schedules were created
        self.assertEqual(ScheduleExec.objects.count(), 0)
        self.run_workers_batch(queue="schedules")
        self.assertEqual(ScheduleExec.objects.count(), 1)

    def test_tasks(self):

        TaskExec.objects.create(task_name="create_user")

        # Ensure the task really was just executed once
        self.assertEqual(User.objects.count(), 0)
        self.run_workers_batch(queue="tasks")
        self.assertEqual(User.objects.count(), 1)
