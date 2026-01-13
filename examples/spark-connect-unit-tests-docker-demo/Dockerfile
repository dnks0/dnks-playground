ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim

ARG SPARK_VERSION=4.0.1

WORKDIR /tests

# Make installs deterministic-ish.
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir \
    pyspark-connect==$SPARK_VERSION \
    pytest \
    psycopg2-binary

COPY tests /tests
ENV PYTHONPATH=/tests
