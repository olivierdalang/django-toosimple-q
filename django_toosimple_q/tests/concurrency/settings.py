"""Settings overrides for testing concurrent workers, uses toxiproxy which simulates latency"""


from ..settings import *

# The background workers must also use the test database
DATABASES["default"].update(DATABASES["default"]["TEST"])

# On postgres, background workers connect through toxiproxy to simluate latency
if is_postgres():
    DATABASES["default"]["HOST"] = os.environ.get("POSTGRES_HOST_WORKER", "127.0.0.1")
    DATABASES["default"]["PORT"] = os.environ.get("POSTGRES_PORT_WORKER", "5433")
