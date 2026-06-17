from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("BronzeLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce-events")
    .option("startingOffsets", "latest")
    .load()
)

bronze_df = df.select(
    col("value").cast("string").alias("raw_event"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)

query = (
    bronze_df.writeStream
    .format("parquet")
    .option(
        "path",
        "/home/snahal/ecommerce-lakehouse/bronze"
    )
    .option(
        "checkpointLocation",
        "/home/snahal/ecommerce-lakehouse/checkpoints/bronze"
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()