from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BronzeLayer")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce-events")
    .option("startingOffsets", "latest")
    .load()
)

query = (
    df.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()