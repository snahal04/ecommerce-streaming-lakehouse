from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import to_date
# Make sure Spark's DataFrame class is imported
from pyspark.sql import DataFrame  
from pyspark.sql import SparkSession
# import sys
# import os

# # 1. Get the absolute path of the directory containing the script (consumer/)
# script_dir = os.path.dirname(os.path.abspath(__file__))

# # 2. Get the parent directory (ecommerce-lakehouse/)
# project_root = os.path.dirname(script_dir)

# # 3. Add the project root to Python's search path if it's not already there
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)


# from common.debug_utils import log_processed_batch

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
    "event_time",
    to_timestamp("event_time")
)

silver_df = silver_df.withWatermark(
    "event_time",
    "2 minutes"
)

silver_df = silver_df.dropDuplicates(
    ["event_id", "event_time"]
)


silver_df = silver_df.withColumn(
    "event_date",
    to_date("event_time")
)

# from common.quality_checks import (
#     validate_records,
#     quarantine_records
# )

# valid_df = validate_records(silver_df)
# bad_df = quarantine_records(silver_df)

# bad_query = (
#     bad_df.writeStream
#     .format("parquet")
#     .partitionBy("event_date")
#     .option(
#         "path",
#         "/home/snahal/ecommerce-lakehouse/quarantine"
#     )
#     .option(
#         "checkpointLocation",
#         "/home/snahal/ecommerce-lakehouse/checkpoints/quarantine"
#     )
#     .outputMode("append")
#     .trigger(processingTime="20 seconds")
#     .start()
# )

# def process_and_log_silver(batch_df: DataFrame, batch_id: int):
#     # 1. Log the tracking details to the console using your common tool
#     log_processed_batch(batch_df, batch_id)
    
#     # 2. Write the actual data to your silver storage (Move partitionBy here!)
#     if not batch_df.isEmpty():
#         batch_df.write \
#             .format("parquet") \
#             .partitionBy("event_date") \
#             .mode("append") \
#             .save("/home/snahal/ecommerce-lakehouse/silver")

def process_save_and_print(batch_df: DataFrame, batch_id: int):
    if not batch_df.isEmpty():
        # 1. Select the columns directly (no get_json_object needed, user_id is already parsed!)
        print_df = batch_df.select(
            col("event_id"),
            col("user_id")
        )
        
        # 2. Print the console summary table
        print(f"\n--- [Processing from last Checkpoint or new micro-batch Every 20 Seconds: {batch_id}] ---")
        print_df.show(20, truncate=False)
        print("-------------------------------------------\n")

        batch_df.write \
            .format("parquet") \
            .partitionBy("event_date") \
            .mode("append") \
            .save("/home/snahal/ecommerce-lakehouse/silver")

query = (
    silver_df.writeStream
    # .format("parquet")
    .foreachBatch(process_save_and_print)
    # .option(
    #     "path",
    #     "/home/snahal/ecommerce-lakehouse/silver"
    # )
    .option(
        "checkpointLocation",
        "/home/snahal/ecommerce-lakehouse/checkpoints/silver"
    )
    .outputMode("append")
    # Added trigger to process the stream every 20 seconds instead of processing as soon as data arrives
    .trigger(processingTime="20 seconds")
    .start()
)

query.awaitTermination()
# Await both terminations so the script stays alive for both streams
# spark.streams.awaitAnyTermination()