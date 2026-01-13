import json
import os


def _ensure_writable_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    # The shared volume is also mounted into the Spark container which runs as UID 185.
    # When we create directories here (as root in the tests container), they default to 755
    # and Spark may not be able to create sibling dirs (checkpoint/output) under them.
    try:
        os.chmod(path, 0o777)
    except PermissionError:
        # Best-effort; in some environments chmod may not be allowed.
        pass


def _write_jsonl(path: str, rows: list[dict]) -> None:
    d = os.path.dirname(path)
    p = os.path.dirname(d)
    _ensure_writable_dir(p)
    _ensure_writable_dir(d)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def test_streaming_trigger_available_now_to_delta(spark, env, unique_id):
    base = f"{env['stream_base']}/{unique_id}"
    input_dir = f"{base}/input"
    output_dir = f"{base}/output_delta"
    checkpoint_dir = f"{base}/checkpoint"

    # Seed input (file source streaming is deterministic and works well in CI).
    _write_jsonl(f"{input_dir}/batch1.json", [{"id": 1, "txt": "a"}, {"id": 2, "txt": "b"}])
    _write_jsonl(f"{input_dir}/batch2.json", [{"id": 3, "txt": "c"}])

    df = spark.readStream.schema("id INT, txt STRING").json(input_dir)

    query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start(output_dir)
    )

    query.awaitTermination(60)

    result = spark.read.format("delta").load(output_dir)
    assert result.count() == 3
