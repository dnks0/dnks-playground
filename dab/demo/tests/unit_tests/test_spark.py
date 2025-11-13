from unittest import TestCase
from pyspark.sql import SparkSession

from dnks_dab_demo.spark import initialize_spark


class TestSpark(TestCase):
    def test_spark(self):
        spark = initialize_spark()
        self.assertTrue(isinstance(spark, SparkSession))
