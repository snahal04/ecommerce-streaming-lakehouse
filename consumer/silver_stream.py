from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import to_date
# Make sure Spark's DataFrame class is imported
from pyspark.sql import DataFrame  
from pyspark.sql import SparkSession
import sys
import os

# 1. Get the absolute path of the directory containing the script (consumer/)
script_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Get the parent directory (ecommerce-lakehouse/)
project_root = os.path.dirname(script_dir)

# 3. Add the project root to Python's search path if it's not already there
if project_root not in sys.path:
    sys.path.insert(0, project_root)


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

# 2. Parsing and Stateful Transformations (Watermarking + Deduplication)
parsed = bronze_stream.withColumn("parsed_json", from_json("raw_event", event_schema))
silver_df = parsed.select("parsed_json.*")

silver_df = (
    silver_df
    .withColumn("event_time", to_timestamp("event_time"))
    .withWatermark("event_time", "2 minutes")
    .dropDuplicates(["event_id", "event_time"])
    .withColumn("event_date", to_date("event_time"))
)

from common.quality_checks import get_valid_records, get_invalid_records

# 3. Single Unified Processing Function
def process_unified_batch(batch_df: DataFrame, batch_id: int):
    # Instead of isEmpty(), cache the dataframe if it's reused multiple times
    batch_df.cache()
    
    # Check count instead of isEmpty to avoid extra action overhead or use try/except
    if batch_df.rdd.isEmpty():
        batch_df.unpersist()
        return

    print(f"\n--- [Processing Micro-batch Every 20 Seconds: {batch_id}] ---")
    
    # Split the batch data using your quality checks
    valid_batch = get_valid_records(batch_df)
    bad_batch = get_invalid_records(batch_df)

    # Handle BAD records (Quarantine)
    if not bad_batch.rdd.isEmpty():
        print(f"Writing invalid records to quarantine...")
        (bad_batch.write
         .format("parquet")
         .partitionBy("event_date")
         .mode("append")
         .save("/home/snahal/ecommerce-lakehouse/quarantine"))

    # Handle VALID records (Silver)
    if not valid_batch.rdd.isEmpty():
        # Print console summary table
        print_df = valid_batch.select(col("event_id"), col("user_id"))
        print_df.show(20, truncate=False)
        
        print(f"Writing valid records to silver layer...")
        (valid_batch.write
         .format("parquet")
         .partitionBy("event_date")
         .mode("append")
         .save("/home/snahal/ecommerce-lakehouse/silver"))
         
    print("-------------------------------------------\n")
    
    # Free memory allocation
    batch_df.unpersist()

# 4. Start the SINGLE streaming query
unified_query = (
    silver_df.writeStream
    .foreachBatch(process_unified_batch)
    .option("checkpointLocation", "/home/snahal/ecommerce-lakehouse/checkpoints/silver_unified")
    .outputMode("append")
    .trigger(processingTime="20 seconds")
    .start()
)

# Wait for the single stream to terminate
unified_query.awaitTermination()