from pyspark.sql import functions as F


def test_delta_read_write(spark, env, unique_id):
    path = f"{env['delta_base']}/basic/{unique_id}"

    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "txt"]).withColumn(
        "txt", F.col("txt")
    )

    df.write.format("delta").mode("overwrite").save(path)

    out = spark.read.format("delta").load(path)
    assert out.count() == 3
    assert {r["id"] for r in out.select("id").collect()} == {1, 2, 3}
