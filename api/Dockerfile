###############################################################################
# builder
###############################################################################
FROM python:3.12.7-slim-bookworm AS builder

RUN pip install poetry==1.8.4

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /venv

COPY pyproject.toml poetry.lock ./

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

###############################################################################
# api
###############################################################################
FROM python:3.12.7-slim-bookworm AS api

ENV MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI
ENV PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

ENV VIRTUAL_ENV=/venv/.venv \
    PATH="/venv/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

RUN apt-get update & apt-get clean
RUN rm -rf /var/lib/apt/lists/*

COPY ./ /app

CMD uvicorn main:app --host 0.0.0.0 --port $PORT --timeout-keep-alive 90
