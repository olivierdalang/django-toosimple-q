import io
from contextlib import redirect_stderr, redirect_stdout

from django.conf import settings
from django.core import management
from django.test import TestCase, override_settings
from django.utils.log import configure_logging
from freezegun import freeze_time


class LoggingTestCase(TestCase):
    # def setUp(self):
    #     logger.handlers = []

    @freeze_time("2020-01-01")
    def test_default_logging(self):
        """With no specific config, log to console"""
        configure_logging(settings.LOGGING_CONFIG, settings.LOGGING)
        buf = io.StringIO()
        with redirect_stdout(buf), redirect_stderr(buf):
            management.call_command("worker", "--once", "--queue", "nop")
        self.assertIn(
            "[2020-01-01 00:00:00][INFO][toosimpleq] Exiting loop because --once was passed",
            buf.getvalue(),
        )

    @freeze_time("2020-01-01")
    @override_settings(
        LOGGING={
            "version": 1,
            "disable_existing_loggers": False,
            "handlers": {"file": {"class": "logging.NullHandler"}},
            "root": {"handlers": ["file"]},
        }
    )
    def test_logging_output(self):
        """With explicit config, handler isn't added"""
        configure_logging(settings.LOGGING_CONFIG, settings.LOGGING)

        buf = io.StringIO()
        with redirect_stdout(buf), redirect_stderr(buf):
            management.call_command("worker", "--once", "--queue", "nop")
        self.assertEqual(
            "",
            buf.getvalue(),
        )
