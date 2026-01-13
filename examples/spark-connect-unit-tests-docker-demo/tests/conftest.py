import os
import socket
import time
import uuid

import psycopg2
import pytest
from pyspark.sql import SparkSession


def _wait_for_tcp(host: str, port: int, timeout_s: int = 60) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError as e:
            last_err = e
            time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for {host}:{port} to accept TCP connections: {last_err}")


def _wait_for_postgres(host: str, port: int, db: str, user: str, password: str, timeout_s: int = 60) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
            conn.close()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for Postgres to be ready: {last_err}")


@pytest.fixture(scope="session")
def env():
    return {
        "spark_remote": os.environ.get("SPARK_REMOTE", "sc://spark:15002"),
        "delta_base": os.environ.get("DELTA_BASE_PATH", "/opt/shared/delta"),
        "stream_base": os.environ.get("STREAM_BASE_PATH", "/opt/shared/streaming"),
        "pg_host": os.environ.get("POSTGRES_HOST", "postgres"),
        "pg_port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "pg_db": os.environ.get("POSTGRES_DB", "testdb"),
        "pg_user": os.environ.get("POSTGRES_USER", "test"),
        "pg_password": os.environ.get("POSTGRES_PASSWORD", "test"),
    }


@pytest.fixture(scope="session")
def spark(env):
    # Wait for Spark Connect TCP port; Connect server can take a bit to bootstrap.
    host = env["spark_remote"].split("//", 1)[1].split(":", 1)[0]
    port = int(env["spark_remote"].rsplit(":", 1)[1])
    _wait_for_tcp(host, port, timeout_s=120)

    # Create Spark Connect session and verify it's responsive.
    deadline = time.time() + 120
    last_err: Exception | None = None
    session: SparkSession | None = None
    while time.time() < deadline:
        try:
            session = SparkSession.builder.remote(env["spark_remote"]).getOrCreate()
            session.sql("select 1").collect()
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1)

    if session is None:
        raise RuntimeError(f"Failed to create SparkSession via Spark Connect: {last_err}")

    yield session

    try:
        session.stop()
    except Exception:
        # Best-effort; connect stop can fail if server is already down.
        pass


@pytest.fixture(scope="session", autouse=True)
def postgres_ready(env):
    _wait_for_tcp(env["pg_host"], env["pg_port"], timeout_s=60)
    _wait_for_postgres(
        env["pg_host"],
        env["pg_port"],
        env["pg_db"],
        env["pg_user"],
        env["pg_password"],
        timeout_s=60,
    )


@pytest.fixture
def unique_id() -> str:
    return uuid.uuid4().hex
