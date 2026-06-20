from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SilverLayer")
    .getOrCreate()
)

bronze_schema = StructType([
    StructField("raw_event", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("timestamp", TimestampType(), True)
])

bronze_stream = (
    spark.readStream
    .schema(bronze_schema)
    .parquet("/home/snahal/ecommerce-lakehouse/bronze")
)

spark.sparkContext.setLogLevel("ERROR")
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("session_id", IntegerType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("amount", DoubleType())
])


parsed = bronze_stream.withColumn(
    "parsed_json",
    from_json("raw_event", event_schema)
)

silver_df = parsed.select(
    "parsed_json.*"
)

silver_df = silver_df.withColumn(
    "event_timestamp",
    to_timestamp("event_time")
)

silver_df = silver_df.withWatermark(
    "event_timestamp",
    "10 minutes"
)

silver_df = silver_df.dropDuplicates(
    ["event_id"]
)

# Idempotency
silver_df = silver_df.dropDuplicates(
    ["event_id"]
)

silver_df = silver_df.withColumn(
    "event_time",
    to_timestamp("event_time")
)


query = (
    silver_df.writeStream
    .format("parquet")
    .option(
        "path",
        "/home/snahal/ecommerce-lakehouse/silver"
    )
    .option(
        "checkpointLocation",
        "/home/snahal/ecommerce-lakehouse/checkpoints/silver"
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()