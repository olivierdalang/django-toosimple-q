"""Settings overrides for testing concurrent workers by simulating latency with  toxiproxy which"""

from .settings_bg import *

# On postgres, background workers connect through toxiproxy to simluate latency
if is_postgres():
    DATABASES["default"]["HOST"] = os.environ.get("POSTGRES_HOST_WORKER", "127.0.0.1")
    DATABASES["default"]["PORT"] = os.environ.get("POSTGRES_PORT_WORKER", "5433")
