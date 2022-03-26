import os
import subprocess


def sys_call(cmd, check=True):
    environ = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "django_toosimple_q.tests.concurrency.settings",
    }
    try:
        process = subprocess.run(
            cmd, capture_output=True, encoding="utf-8", env=environ
        )
    except subprocess.CalledProcessError as e:
        if check:
            raise Exception(e.output)
        return e
    return process


def prepare_toxiproxy():
    # Create a toxic (with artificial latency) proxy to database on port 5444
    sys_call(["docker", "stop", "toxiproxy"], check=False)
    sys_call(
        [
            "docker",
            "run",
            "--name",
            "toxiproxy",
            "-p",
            "5444:5444",
            "--add-host",
            "host.docker.internal:host-gateway",
            "-d",
            "--rm",
            "ghcr.io/shopify/toxiproxy",
        ]
    )
    sys_call(
        [
            "docker",
            "exec",
            "toxiproxy",
            "/toxiproxy-cli",
            "create",
            "-l",
            "0.0.0.0:5444",
            "-u",
            "host.docker.internal:5432",
            "postgres",
        ]
    )
    sys_call(
        [
            "docker",
            "exec",
            "toxiproxy",
            "/toxiproxy-cli",
            "toxic",
            "add",
            "-t",
            "latency",
            "-n",
            "my_lag",
            "-a",
            "latency=100",
            "-a",
            "jitter=5",
            "postgres",
        ]
    )
