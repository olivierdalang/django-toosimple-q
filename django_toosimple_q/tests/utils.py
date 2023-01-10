import os


def is_postgres():
    return os.getenv("TOOSIMPLEQ_TEST_DB", None) == "postgres"
