ARG TOOSIMPLEQ_PY_VERSION

FROM python:$TOOSIMPLEQ_PY_VERSION

WORKDIR /app

# Install empty project (source added/mounted later)
ENV SETUPTOOLS_SCM_PRETEND_VERSION 0.0.0
ADD pyproject.toml ./
RUN touch README.md
RUN mkdir django_toosimple_q
RUN pip install -e .[dev]

# Override django version
ARG TOOSIMPLEQ_DJ_VERSION
RUN pip install Django==$TOOSIMPLEQ_DJ_VERSION

# Add source files
ADD . /app

# Default command runs tests
ENTRYPOINT ["python", "manage.py"]
CMD ["--help"]
