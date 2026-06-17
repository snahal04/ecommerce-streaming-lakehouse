from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("KafkaTest")
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

parsed = df.select(
    col("value").cast("string").alias("message")
)

query = (
    parsed.writeStream
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()