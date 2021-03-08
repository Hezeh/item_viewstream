FROM tiangolo/uvicorn-gunicorn:python3.8-slim

RUN pip install --no-cache-dir fastapi[all] google-cloud-pubsub confluent-kafka

COPY ./app /app