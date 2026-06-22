from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import sys
import os

# 1. Get the absolute path of the directory containing the script (consumer/gold/)
script_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Go up one level to (consumer/)
consumer_dir = os.path.dirname(script_dir)

# 3. Go up another level to get to the true root (ecommerce-lakehouse/)
project_root = os.path.dirname(consumer_dir)

# 4. Add the true project root to Python's search path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now this import will find your schema perfectly!
from common.silver_schema import silver_schema

spark = (
    SparkSession.builder
    .appName("UserMetrics")
    .config("spark.log.level", "WARN")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

silver_stream = (
    spark.readStream
    .schema(silver_schema)
    .parquet("/home/snahal/ecommerce-lakehouse/silver")
)

gold_df = (
    silver_stream
    .groupBy("user_id")
    .agg(
        count("*").alias("total_events"),

        # FIXED: Changed countDistinct to approx_count_distinct
        approx_count_distinct("session_id").alias("total_sessions"),

        round(
            sum("amount"),
            2
        ).alias("total_spend")
    )
)

def save_batch(batch_df: DataFrame, batch_id: int):

    print(f"\n----- User Metrics Batch {batch_id} -----")
    batch_df.show(truncate=False)

    (
        batch_df.write
        .mode("overwrite")
        .parquet(
            "/home/snahal/ecommerce-lakehouse/gold/user_metrics"
        )
    )

query = (
    gold_df.writeStream
    .foreachBatch(save_batch)
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "/home/snahal/ecommerce-lakehouse/checkpoints/gold_user_metrics"
    )
    .trigger(processingTime="20 seconds")
    .start()
)

query.awaitTermination()