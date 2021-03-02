import os
import sys

basedir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(basedir)

import django_toosimple_q  # noqa

version = django_toosimple_q.__version__
tag = os.getenv("GITHUB_TAG")

assert (
    f"refs/heads/{version}" == tag or f"refs/tags/{version}" == tag
), f"Version mismatch : {version} != {tag}"
