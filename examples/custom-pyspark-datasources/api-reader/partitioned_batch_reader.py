# Databricks notebook source
# MAGIC %md
# MAGIC ### **Custom PySpark Data-Source with partitioning: Read from (REST)-API**

# COMMAND ----------

# MAGIC %md
# MAGIC - Preview
# MAGIC - Available from DBR 15.2 (Batch), from 15.3 (Streaming)
# MAGIC - Flexible and standardized Interface
# MAGIC - Reusability while hiding complexity

# COMMAND ----------

import ast
import json
import requests

from pyspark.sql import types as T, functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

# COMMAND ----------

# MAGIC %md
# MAGIC #### Implement InputPartition, DataSourceReader & DataSource

# COMMAND ----------

class GeoPartition(InputPartition):
  def __init__(self, lat, long):
        self.lat = lat
        self.long = long

# COMMAND ----------

class APIDataSourceReader(DataSourceReader):

    def __init__(
      self,
      schema: T.StructType,
      options: dict[str, str]
    ):
        self.schema = schema
        self.options = options
        self.locations = self._parse_locations(options.get("locations", "[]"))
        self.api_key = options.get("apikey", "")
        self.session = requests.Session()  # Use a session for connection pooling
        self.url = "https://api.tomorrow.io/v4/weather/forecast"

    def partitions(self):
      return [GeoPartition(lat, long) for lat, long in self.locations]  # return N elements, results in N tasks --> N partitions !

    @staticmethod
    def _parse_locations(locations_str: str):
        """Converts string representation of list of tuples to actual list 
        of tuples."""
        return [tuple(map(float, x)) for x in ast.literal_eval(locations_str)]

    @staticmethod
    def _fetch_weather(url: str, lat: float, long: float, api_key: str, session: requests.Session):
        """Fetches weather data for the given latitude and longitude using a REST API."""
        url = f"{url}?location={lat},{long}&apikey={api_key}"
        response = session.get(url)
        response.raise_for_status()
        return response.json()["timelines"]

    def read(self, partition: InputPartition):
        """Reads data from the API and returns a sequence of rows."""
        data = self._fetch_weather(
            url=self.url,
            lat=partition.lat,
            long=partition.long,
            api_key=self.api_key,
            session=self.session
        )["minutely"]

        for entry in data:
            yield (
                entry["time"],
                partition.lat,
                partition.long,
                json.dumps(entry["values"])
            )

# COMMAND ----------

class APIDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "api"

    def schema(self):
        return T.StructType(
          [
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("latitude", T.DoubleType(), True),
            T.StructField("longitude", T.DoubleType(), True),
            T.StructField("weather", T.StringType(), True),
          ]
        )

    def reader(self, schema: T.StructType):
        return APIDataSourceReader(
            schema=schema,
            options=self.options
        )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register DataSource

# COMMAND ----------

spark.dataSource.register(APIDataSource)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Usage

# COMMAND ----------

catalog = "<fill-in>"
schema = "<fill-in>"

apiKey = "<fill-in>"
locations = """[
    (60.3933, 5.8341),    # Snorre Oil Field, Norway
    (58.757, 2.198),      # Schiehallion, UK
    (58.871, 4.862),      # Clair field, UK
    (57.645, 3.164),      # Elgin-Franklin, UK
    (54.932, -5.498),     # Sean field, UK
    (-14.849, 12.395),    # Angola offshore
    (1.639, 100.468),     # Malampaya, Philippines
    (-27.0454, 152.1213), # Australia offshore
    (38.1, -119.8),       # California offshore
    (52.784, 1.698)       # Leman, North Sea
]"""

# COMMAND ----------

df = (
  spark.read.format("api")
    .option("apiKey", apiKey)
    .option("locations", locations)
    .load()
)


# COMMAND ----------

df.cache()

# COMMAND ----------

df.display()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.withColumn("humidity", F.get_json_object("weather", "$.humidity")).display()

# COMMAND ----------

df.write.saveAsTable(
  name=f"{catalog}.{schema}.weather_raw",
  mode="append"
)