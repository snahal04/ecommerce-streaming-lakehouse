from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType

silver_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", IntegerType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_date", DateType(), True)
])