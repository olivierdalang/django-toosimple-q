from django.core.mail.backends.base import BaseEmailBackend

from .tasks import send_email


class QueueBackend(BaseEmailBackend):
    def __init__(self, **kwargs):
        super().__init__(kwargs)

    def send_messages(self, email_messages):
        send_email.queue(email_messages)
