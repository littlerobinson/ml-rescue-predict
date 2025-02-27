###############################################################################
# builder
###############################################################################
FROM python:3.13.2-slim-bookworm AS builder

RUN pip install poetry==1.8.5

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /venv

COPY pyproject.toml poetry.lock ./

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

###############################################################################
# mlflow
###############################################################################
FROM python:3.13.2-slim-bookworm AS mlflow

# ENV variables
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_ARTIFACT_S3_URI=$AWS_ARTIFACT_S3_URI
ENV BACKEND_STORE_URI=$BACKEND_STORE_URI
ENV PIP_ROOT_USER_ACTION=ignore

WORKDIR /mlflow

# VIRTUAL_ENV = virtual environment path
# PATH = virtual environment executable path
ENV VIRTUAL_ENV=/venv/.venv \
    PATH="/venv/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# Update and install needed packages
RUN apt-get update
RUN apt-get install -y curl unzip git
RUN rm -rf /var/lib/apt/lists/*

# Install AWS CLI for remote use
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscli.zip"
RUN unzip awscli.zip
RUN ./aws/install
RUN rm awscli.zip
RUN rm -rf ./aws

CMD mlflow server --port 7860 \
    --host 0.0.0.0 \
    --backend-store-uri $BACKEND_STORE_URI \
    --default-artifact-root $AWS_ARTIFACT_S3_URI


