from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

productSchema = StructType().add("product_id", "integer").add("name", "string").add("added_dt","string")

productDf = spark.read.csv("product.csv", mode="DROPMALFORMED", schema=productSchema)

streamingDf.join(staticDf, "product_id")  # inner equi-join with a static DF
streamingDf.join(staticDf, "type", "right_join")  # 

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("topic", "topic1") \
  .save()

query.awaitTermination()