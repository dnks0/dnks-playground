from pyspark.sql import SparkSession


def initialize_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
