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
    .appName("DailyRevenue")
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
    .filter(col("event_type") == "purchase")

    .groupBy("event_date")

    .agg(
        count("*").alias("orders"),

        round(
            sum("amount"),
            2
        ).alias("revenue")
    )
)

def save_batch(batch_df: DataFrame, batch_id: int):

    print(f"\n----- Daily Revenue Batch {batch_id} -----")
    batch_df.show(truncate=False)

    (
        batch_df.write
        .mode("overwrite")
        .parquet(
            "/home/snahal/ecommerce-lakehouse/gold/daily_revenue"
        )
    )

query = (
    gold_df.writeStream
    .foreachBatch(save_batch)
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "/home/snahal/ecommerce-lakehouse/checkpoints/gold_daily_revenue"
    )
    .trigger(processingTime="20 seconds")
    .start()
)

query.awaitTermination()