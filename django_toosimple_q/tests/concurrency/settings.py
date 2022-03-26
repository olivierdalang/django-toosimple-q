"""Minimal settings for testing concurrent workers"""

DEBUG = True
USE_TZ = True
TIME_ZONE = "UTC"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "127.0.0.1",
        "PORT": "5444",  # go through toxiproxy for artifical latency
        "NAME": "test_postgres",
        "USER": "postgres",
        "PASSWORD": "postgres",
    }
}

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django_toosimple_q",
    "django_toosimple_q.tests.concurrency",
]
