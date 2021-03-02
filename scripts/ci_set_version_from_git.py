import os

basedir = os.path.dirname(os.path.dirname(__file__))

name = os.getenv("GITHUB_TAG").split("/")[2]
path = os.path.join(basedir, "django_toosimple_q", "__init__.py")

# read file
contents = open(path, "r").read()

# replace contents
open(path, "w").write(contents.replace("dev", name, 1))
