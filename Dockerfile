ARG TOOSIMPLEQ_PY_VERSION

FROM python:$TOOSIMPLEQ_PY_VERSION

WORKDIR /app

# Install app in editable mode
ADD ./requirements.txt /app/requirements.txt
ADD ./requirements-dev.txt /app/requirements-dev.txt
RUN touch /app/README.md
ADD ./django_toosimple_q/__init__.py /app/django_toosimple_q/__init__.py
ADD ./setup.py /app/setup.py
RUN pip install -r requirements-dev.txt

# Override django version
ARG TOOSIMPLEQ_DJ_VERSION
RUN pip install Django==$TOOSIMPLEQ_DJ_VERSION

# Add source files
ADD . /app

# Default command runs tests
ENTRYPOINT ["python", "manage.py"]
CMD ["--help"]
