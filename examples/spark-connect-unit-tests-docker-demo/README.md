## Spark Connect + Dockerized unit tests (PySpark)

A lean, dockerized end-to-end demo for running **pytest** against a **Spark Connect** server using **PySpark Connect**, covering:

- **Delta Lake**: read/write
- **JDBC (Postgres)**: read/write
- **Structured Streaming**: deterministic file source → Delta sink

### What you get

- **Spark Connect server**: `apache/spark:4.0.1` (Scala 2.13), started via `start-connect-server.sh`
- **Delta Lake jars**: pulled at runtime via `--packages` (`io.delta:delta-spark_2.13:4.0.1`)
- **Test runner**: a tiny Python image that installs `pyspark-connect==${SPARK_VERSION}` (+ `pytest`, `psycopg2-binary`)
- **Shared data volume**: mounted at **`/opt/data`** in both Spark + tests so the server can see the files the tests write

### Run

From the repo root:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from demo-test-runner demo-test-runner
```

Clean up containers (and optionally the volume):

```bash
docker compose down        # keep /opt/data volume
docker compose down -v     # also delete the data volume
```

### Services (docker-compose)

- **`demo-spark-connect-server`**: Spark Connect on port `15002`
- **`demo-postgres`**: Postgres on port `5432`
- **`demo-test-runner`**: runs `pytest` inside the container

### Configuration knobs

In `docker-compose.yml`:

- **`demo-test-runner.build.args.PYTHON_VERSION`**: Python base image version (default `3.12`)
- **`demo-test-runner.build.args.SPARK_VERSION`**: `pyspark-connect` version to install (default `4.0.1`)

Paths:

- **`DELTA_BASE_PATH`**: `/opt/data/delta`
- **`STREAM_BASE_PATH`**: `/opt/data/streaming`
- **Ivy cache**: `/opt/data/ivy` (shared so subsequent runs are fast)

### Troubleshooting

- **Spark Connect not starting**: check logs
  - `docker compose logs -f demo-spark-connect-server`
- **Package downloads failing**: ensure Docker engine has outbound internet; jars are cached under `/opt/data/ivy`
- **Why Spark 4.0.1?**: it’s the latest Spark line where published Delta jars work cleanly with Spark Connect
