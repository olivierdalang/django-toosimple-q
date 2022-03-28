import os
import subprocess


def is_postgres():
    return os.getenv("TOOSIMPLEQ_TEST_DB", None) == "postgres"


def prepare_toxiproxy():
    # Create a toxic (with artificial latency) proxy to database on port 5444
    subprocess.run(["docker", "stop", "toxiproxy"], check=False)
    subprocess.run(
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
        ],
        check=True,
    )
    subprocess.run(
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
        ],
        check=True,
    )
    subprocess.run(
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
        ],
        check=True,
    )
