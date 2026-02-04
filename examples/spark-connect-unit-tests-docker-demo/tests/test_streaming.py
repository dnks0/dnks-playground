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


def test_streaming_foreach_batch_merges_delta(spark, env, unique_id):
    base = f"{env['stream_base']}/{unique_id}"
    input_dir = f"{base}/input"
    output_dir = f"{base}/foreach_batch_delta"
    checkpoint_dir = f"{base}/checkpoint"

    # Seed input for two batches.
    _write_jsonl(f"{input_dir}/batch1.json", [{"id": 10, "txt": "x"}, {"id": 20, "txt": "y"}])
    _write_jsonl(f"{input_dir}/batch2.json", [{"id": 30, "txt": "z"}])

    # Seed the target Delta table with one row to exercise MERGE updates.
    seed = spark.createDataFrame([(10, "old")], ["id", "txt"])
    seed.write.format("delta").mode("overwrite").save(output_dir)

    df = spark.readStream.schema("id INT, txt STRING").json(input_dir)

    def write_batch(batch_df, batch_id):
        batch_df.createOrReplaceTempView("updates")
        spark.sql(
            "MERGE INTO delta.`{path}` t "
            "USING updates s "
            "ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *".format(path=output_dir)
        )

    query = (
        df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination(60)

    result = spark.read.format("delta").load(output_dir)
    assert result.count() == 3
    assert result.where("id = 10 AND txt = 'x'").count() == 1
