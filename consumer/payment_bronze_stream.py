from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, get_json_object

# Initialize Spark Session optimized for localized execution
spark = (
    SparkSession.builder
    .appName("PaymentBronzeLayer")
    .master("local[*]")
    .getOrCreate()
)

# Suppress verbose log noise to keep console summaries clean
spark.sparkContext.setLogLevel("ERROR")

# Read streaming payload from your local Kafka broker
payment_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "payment-events")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Convert metadata and binary payload explicitly into the bronze framework
bronze_df = payment_df.select(
    col("value").cast("string").alias("raw_event"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)

def process_save_and_print(batch_df: DataFrame, batch_id: int):
    """
    Micro-batch execution block to print live console telemetry 
    and append incoming payloads cleanly onto your local disk.
    """
    if not batch_df.isEmpty():
        # 1. Peek inside the JSON string to grab user identification data
        print_df = batch_df.select(
            get_json_object(col("raw_event"), "$.user_id").alias("user_id"),
            col("partition"),
            col("offset")
        )
        
        # 2. Print structured metrics table into the active console terminal
        print(f"\n--- [Streaming from last Checkpoint or new batch: {batch_id}] ---")
        print_df.show(20, truncate=False)
        print("-------------------------------------------\n")

        # 3. Micro-batch write targeting the physical payment storage block
        batch_df.write \
            .format("parquet") \
            .mode("append") \
            .save("/home/snahal/ecommerce-lakehouse/payment_bronze")

# Initialize streaming runner with explicit foreachBatch logic execution blocks
query = (
    bronze_df.writeStream
    .foreachBatch(process_save_and_print)
    .option(
        "checkpointLocation",
        "/home/snahal/ecommerce-lakehouse/checkpoints/payment_bronze"
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()