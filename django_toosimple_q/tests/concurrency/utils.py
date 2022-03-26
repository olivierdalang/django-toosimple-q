import os
import subprocess


def docker_call(cmd, check=True):
    sys_call(f"docker {cmd}", check)


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
    docker_call("stop toxiproxy", check=False)
    docker_call("run --name toxiproxy -p 5444:5444 -d --rm ghcr.io/shopify/toxiproxy")
    docker_call(
        "exec toxiproxy /toxiproxy-cli create -l 0.0.0.0:5444 -u host.docker.internal:5432 postgres"
    )
    docker_call(
        "exec toxiproxy /toxiproxy-cli toxic add -t latency -n my_lag -a latency=100 -a jitter=5 postgres"
    )
