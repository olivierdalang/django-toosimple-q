"""Settings overrides for testing concurrent workers, uses toxiproxy which simulates latency"""

import os

from ..settings import *

# The background workers must also use the test database
DATABASES["default"].update(DATABASES["default"]["TEST"])

# On postgres, background workers connect through toxiproxy to simluate latency
if os.getenv("TOOSIMPLEQ_TEST_DB", "sqlite") == "postgres":
    DATABASES["default"]["PORT"] = "5444"
