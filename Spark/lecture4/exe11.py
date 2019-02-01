from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

productSchema = StructType().add("id", "integer").add("name", "string")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load()

keyValDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = keyValDf \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
