from pyspark.sql.functions import col, get_json_object
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

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

def process_save_and_print(batch_df: DataFrame, batch_id: int):
    if not batch_df.isEmpty():
        # 1. Extract the user_id from the JSON string for the console print look
        print_df = batch_df.select(
            get_json_object(col("raw_event"), "$.user_id").alias("user_id"),
            col("partition"),
            col("offset")
        )
        
        # 2. Print the console summary table
        print(f"\n--- [Streaming from last Checkpoint or new batch: {batch_id}] ---")
        print_df.show(20, truncate=False)
        print("-------------------------------------------\n")

        batch_df.write \
            .format("parquet") \
            .mode("append") \
            .save("/home/snahal/ecommerce-lakehouse/bronze")


query = (
    bronze_df.writeStream
    # .format("parquet")
    .foreachBatch(process_save_and_print)
    # .option(
    #     "path",
    #     "/home/snahal/ecommerce-lakehouse/bronze"
    # )
    .option(
        "checkpointLocation",
        "/home/snahal/ecommerce-lakehouse/checkpoints/bronze"
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()