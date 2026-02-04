ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-bookworm

ARG SPARK_VERSION=4.0.1

WORKDIR /tests

# Make installs deterministic-ish.
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
  | tar -xz -C /opt \
  && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

RUN pip install --no-cache-dir \
    pyspark-connect==$SPARK_VERSION \
    pytest \
    psycopg2-binary

COPY tests /tests
ENV PYTHONPATH=/tests
