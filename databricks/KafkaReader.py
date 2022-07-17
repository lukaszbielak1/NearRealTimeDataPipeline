# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DoubleType, ArrayType

schema = StructType(
                    [
                        StructField("delay", LongType(), True),
                        StructField("direction", LongType(), True),
                        StructField("generated", StringType(), True),
                        StructField("gpsQuality", LongType(), True),
                        StructField("headsign", StringType(), True),
                        StructField("lat", DoubleType(), True),
                        StructField("lon", DoubleType(), True),
                        StructField("routeShortName", StringType(), True),
                        StructField("scheduledTripStartTime", StringType(), True),
                        StructField("speed", LongType(), True),
                        StructField("tripId", LongType(), True),
                        StructField("vehicleCode", StringType(), True),
                        StructField("vehicleId", LongType(), True),
                        StructField("vehicleService", StringType(), True),
                    ]
                )


# COMMAND ----------

CLOUDKARAFKA_BROKERS = dbutils.secrets.get("kv-default-cloudbi", "CLOUDKARAFKA-BROKERS")
CLOUDKARAFKA_USERNAME = dbutils.secrets.get("kv-default-cloudbi", "CLOUDKARAFKA-USERNAME")
CLOUDKARAFKA_PASSWORD = dbutils.secrets.get("kv-default-cloudbi", "CLOUDKARAFKA-PASSWORD")


# COMMAND ----------

from pyspark.sql.functions import unbase64, col, from_json, explode

df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers",  CLOUDKARAFKA_BROKERS) \
      .option(f"kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{CLOUDKARAFKA_USERNAME}'password='{CLOUDKARAFKA_PASSWORD}';") \
      .option("subscribe", "topicname") \
      .option("group.id","username-consumer") \
      .option("startingOffsets", "earliest") \
      .option("kafka.ssl.endpoint.identification.algorithm", "https") \
      .option("kafka.security.protocol","SASL_SSL") \
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
      .load() \
      .withColumn("str_value", col('value').cast('String'))\
      .withColumn("json", from_json(col("str_value"),schema)) \
      .select(["json.generated", "json.vehicleCode","json.routeShortName", "json.lat", "json.lon","json.headsign"])


# COMMAND ----------

df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .trigger(processingTime='30 seconds') \
  .option("checkpointLocation", "/tmp/delta/bus_monitor/checkpoints/") \
  .toTable("bus_monitor")
