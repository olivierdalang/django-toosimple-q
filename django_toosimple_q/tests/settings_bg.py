"""Settings overrides for testing background workers"""

from .settings import *

# The background workers must also use the test database
DATABASES["default"].update(DATABASES["default"]["TEST"])
