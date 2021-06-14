from django.conf import settings
from django.core.mail import get_connection

from django_toosimple_q.decorators import register_task


@register_task(unique=True, retries=10, retry_delay=3)
def send_email(email):

    backend = getattr(
        settings,
        "TOOSIMPLEQ_EMAIL_BACKEND",
        "django.core.mail.backends.smtp.EmailBackend",
    )

    conn = get_connection(backend=backend)
    conn.open()
    conn.send_messages([email])
    conn.close()
