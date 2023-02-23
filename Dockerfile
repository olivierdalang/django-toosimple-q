ARG TOOSIMPLEQ_PY_VERSION

FROM python:$TOOSIMPLEQ_PY_VERSION

WORKDIR /app

# Install app in editable mode
ADD ./requirements.txt /app/requirements.txt
ADD ./requirements-dev.txt /app/requirements-dev.txt
RUN pip install -r requirements-dev.txt -r requirements.txt

# Override django version
# (using a slightly hacky way to support both powershell and bash)
ARG TOOSIMPLEQ_DJ_VERSION
RUN pip install "Django==${TOOSIMPLEQ_DJ_VERSION}${env:TOOSIMPLEQ_DJ_VERSION}"

# Add source files
ADD . /app

# Default command runs tests
ENTRYPOINT ["python", "manage.py"]
CMD ["--help"]
