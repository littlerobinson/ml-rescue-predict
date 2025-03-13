FROM python:3.12.7-slim-bookworm AS rescue-predict

ENV PIP_ROOT_USER_ACTION=ignore

EXPOSE $PORT

WORKDIR /app

RUN apt-get update && apt-get clean

COPY requirements.txt /dependencies/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /dependencies/requirements.txt

COPY ./src /app/src

CMD uvicorn src.main:app --host 0.0.0.0 --port $PORT