
def test_jdbc_read_write_postgres(spark, env, unique_id):
    table = f"public.demo_{unique_id}"  # Spark will create if missing

    url = f"jdbc:postgresql://{env['pg_host']}:{env['pg_port']}/{env['pg_db']}"
    props = {
        "user": env["pg_user"],
        "password": env["pg_password"],
        "driver": "org.postgresql.Driver",
    }

    df = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "v"])

    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .mode("overwrite")
        .save()
    )

    out = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .load()
    )

    assert out.count() == 2
