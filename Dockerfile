FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir \
    kafka-python \
    faker \
    great_expectations \
    boto3

COPY src/ ./src/

ENV PYTHONUNBUFFERED=1

CMD ["python", "--version"]
