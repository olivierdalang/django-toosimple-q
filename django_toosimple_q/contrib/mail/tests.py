import importlib

from django.core import mail, management
from django.core.mail import send_mail, send_mass_mail
from django.test.utils import override_settings

from ...models import TaskExec
from ...tests.base import TooSimpleQRegularTestCase
from . import tasks as mail_tasks


class TestMail(TooSimpleQRegularTestCase):
    def setUp(self):
        super().setUp()
        # Reload the tasks modules to repopulate the registries (emulates auto-discovery)
        importlib.reload(mail_tasks)

    @override_settings(
        EMAIL_BACKEND="django_toosimple_q.contrib.mail.backend.QueueBackend",
        TOOSIMPLEQ_EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    )
    def test_queue_mail(self):
        self.assertQueue(0)

        send_mail(
            "Subject here",
            "Here is the message.",
            "from@example.com",
            ["to@example.com"],
        )

        self.assertQueue(1, state=TaskExec.States.QUEUED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 1)

    @override_settings(
        EMAIL_BACKEND="django_toosimple_q.contrib.mail.backend.QueueBackend",
        TOOSIMPLEQ_EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    )
    def test_queue_mail_two(self):
        self.assertQueue(0)

        send_mail(
            "Subject here",
            "Here is the message.",
            "from@example.com",
            ["to@example.com"],
        )
        send_mail(
            "Other subject here",
            "Here is the message.",
            "from@example.com",
            ["to@example.com"],
        )

        self.assertQueue(2, state=TaskExec.States.QUEUED)
        self.assertQueue(2)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(2, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(2)
        self.assertEquals(len(mail.outbox), 2)

    @override_settings(
        EMAIL_BACKEND="django_toosimple_q.contrib.mail.backend.QueueBackend",
        TOOSIMPLEQ_EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    )
    def test_queue_mail_duplicate(self):
        self.assertQueue(0)

        send_mail(
            "Subject here",
            "Here is the message.",
            "from@example.com",
            ["to@example.com"],
        )
        send_mail(
            "Subject here",
            "Here is the message.",
            "from@example.com",
            ["to@example.com"],
        )

        self.assertQueue(1, state=TaskExec.States.QUEUED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 1)

    @override_settings(
        EMAIL_BACKEND="django_toosimple_q.contrib.mail.backend.QueueBackend",
        TOOSIMPLEQ_EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    )
    def test_queue_mass_mail(self):
        self.assertQueue(0)

        send_mass_mail(
            [
                ("Subject A", "Message.", "from@example.com", ["to@example.com"]),
                ("Subject B", "Message.", "from@example.com", ["to@example.com"]),
                ("Subject C", "Message.", "from@example.com", ["to@example.com"]),
            ]
        )

        self.assertQueue(1, state=TaskExec.States.QUEUED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=TaskExec.States.SUCCEEDED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 3)

    @override_settings(
        EMAIL_BACKEND="django_toosimple_q.contrib.mail.backend.QueueBackend",
        TOOSIMPLEQ_EMAIL_BACKEND="failing_backend",
    )
    def test_queue_mail_failing_backend(self):
        self.assertQueue(0)

        send_mail(
            "Subject here",
            "Here is the message.",
            "from@example.com",
            ["to@example.com"],
        )

        self.assertQueue(1, state=TaskExec.States.QUEUED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=TaskExec.States.FAILED, replaced=True)
        self.assertQueue(1, state=TaskExec.States.SLEEPING)
        self.assertQueue(2)
        self.assertEquals(len(mail.outbox), 0)
