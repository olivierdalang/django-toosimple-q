from django.core import mail, management
from django.core.mail import send_mail
from django.test import TestCase
from django.test.utils import override_settings

from django_toosimple_q.models import Task

from .utils import QueueAssertionMixin


class TestMail(QueueAssertionMixin, TestCase):
    def setUp(self):
        mail.outbox.clear()

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

        self.assertQueue(1, state=Task.QUEUED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=Task.SUCCEEDED)
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

        self.assertQueue(2, state=Task.QUEUED)
        self.assertQueue(2)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(2, state=Task.SUCCEEDED)
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

        self.assertQueue(1, state=Task.QUEUED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=Task.SUCCEEDED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 1)

    @override_settings(
        EMAIL_BACKEND="django_toosimple_q.contrib.mail.backend.QueueBackend",
        TOOSIMPLEQ_EMAIL_BACKEND="failing_backend",
    )
    def test_queue_mail(self):

        self.assertQueue(0)

        send_mail(
            "Subject here",
            "Here is the message.",
            "from@example.com",
            ["to@example.com"],
        )

        self.assertQueue(1, state=Task.QUEUED)
        self.assertQueue(1)
        self.assertEquals(len(mail.outbox), 0)

        management.call_command("worker", "--until_done")

        self.assertQueue(1, state=Task.FAILED, replaced=True)
        self.assertQueue(1, state=Task.SLEEPING)
        self.assertQueue(2)
        self.assertEquals(len(mail.outbox), 0)
