"""Settings for running tests"""

import os

from .utils import is_postgres

DEBUG = True
USE_TZ = True
TIME_ZONE = "UTC"
SECRET_KEY = "secret_key"

if is_postgres():
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "HOST": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            "PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "NAME": "postgres",
            "USER": "postgres",
            "PASSWORD": "postgres",
            "TEST": {
                "NAME": "test_postgres",
            },
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": "db.sqlite3",
            "OPTIONS": {"timeout": 50},
            "TEST": {
                "NAME": "db-test.sqlite3",
            },
        }
    }

INSTALLED_APPS = [
    "django_toosimple_q.tests.concurrency",
    "django_toosimple_q.tests.demo",
    "django_toosimple_q",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.messages",
    "django.contrib.sessions",
    "django.contrib.staticfiles",
]

ROOT_URLCONF = "django_toosimple_q.tests.urls"

MIDDLEWARE = (
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
)

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "django.template.context_processors.request",
            ]
        },
    }
]

STATIC_URL = "/static/"
