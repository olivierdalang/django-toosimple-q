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

from ..logging import logger
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
        command = ["python", "manage.py", "worker", "--until_done"]
        if queue:
            command.extend(["--queue", queue])

        outputs = Parallel(n_jobs=COUNT)(
            delayed(sys_call)(command) for i in range(COUNT)
        )

        # Ensure no worker failed
        errored_ouputs = [o for o in outputs if o.returncode != 0]
        if errored_ouputs:
            logger.exception(f"Last output:")

            def indent(txt):
                return "\n".join([" >  " + l for l in txt.splitlines()])

            logger.exception("\n" + indent(errored_ouputs[-1].stdout))
            logger.exception("\n" + indent(errored_ouputs[-1].stderr))
            raise AssertionError(
                f"{len(errored_ouputs)} out of {len(outputs)} workers errored!"
            )

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
