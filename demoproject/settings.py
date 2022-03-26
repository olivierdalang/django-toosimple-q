import os

DEBUG = True
USE_TZ = True
TIME_ZONE = "UTC"

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "_ijq6*+k4!wxrkh8=ps%-qz(f-0m-q)f5qemnc5cck7usceg5j"

if os.getenv("TOOSIMPLEQ_TEST_DB", "sqlite") == "postgres":
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "HOST": "127.0.0.1",
            "PORT": "5432",
            "NAME": "postgres",
            "USER": "postgres",
            "PASSWORD": "postgres",
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": "db.sqlite3",
        }
    }

ROOT_URLCONF = "demoproject.urls"

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sites",
    "django.contrib.messages",
    "django.contrib.sessions",
    "django.contrib.staticfiles",
    "django_toosimple_q",
    # for testing concurrency
    "demoproject.concurrency",
    # demo
    "demoproject.demoapp",
    "django_toosimple_q.contrib.mail",
]

SITE_ID = 1

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

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

STATIC_URL = "/static/"
