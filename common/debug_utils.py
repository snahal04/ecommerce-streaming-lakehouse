# Not using .log() here to avoid Spark's log formatting and potential performance overhead in a streaming context. Instead, we want a clean, custom log output that focuses on the key tracking details of each micro-batch.    
from pyspark.sql import DataFrame

def log_processed_batch(df: DataFrame, batch_id: int):
    """
    Prints a clean log summary of the processed events in the current micro-batch.
    Dynamically checks for Kafka metadata columns.
    """
    if df.isEmpty():
        return

    print(f"\n--- [Processing Micro-Batch: {batch_id}] ---")
    
    # Check what columns are actually available in this DataFrame
    available_cols = df.columns
    has_kafka_meta = "partition" in available_cols and "offset" in available_cols
    
    # Select columns safely
    if has_kafka_meta:
        rows = df.select("event_id", "partition", "offset").limit(20).collect()
        for row in rows:
            print(f"Processed -> Event ID: {row['event_id']} | Partition: {row['partition']} | Offset: {row['offset']}")
    else:
        # Fallback for Silver/Gold layers where Kafka metadata isn't present
        rows = df.select("event_id").limit(20).collect()
        for row in rows:
            print(f"Processed -> Event ID: {row['event_id']}")
            
    print(f"-------------------------------------------\n")