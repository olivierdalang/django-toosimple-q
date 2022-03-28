from django_toosimple_q.tests.settings import *

# Add the demo and contrib apps
INSTALLED_APPS = [
    *INSTALLED_APPS,
    "demoproject.demoapp",
    "django_toosimple_q.contrib.mail",
]
