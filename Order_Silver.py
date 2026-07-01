# Databricks notebook source
# MAGIC %skip
# MAGIC df = spark.read.table("kafka_stream.default.orders_raw")

# COMMAND ----------

# MAGIC %skip
# MAGIC display(df)

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

schema = StructType([
    StructField("order_id", StringType()),
    StructField("cust_id", StringType()),
    StructField("region", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", StringType()),
    StructField("amount", StringType())
])
print(schema)

# COMMAND ----------

# DBTITLE 1,Cell 4
silver_df = (
    spark.readStream
        .table("kafka_stream.default.orders_raw")
        .withColumn("json_data", 
                    from_json(col("value"), schema)
        )
        .select(
            "json_data.*",
            "load_Time",
            "job_id"
        )
)

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.widgets.text("job_id", "")

# COMMAND ----------

job_id_silver = dbutils.widgets.get("job_id")
print(job_id_silver)

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.functions import upper, trim, expr, date_format, current_timestamp, col, lit
valid_orders = (
    silver_df
        .filter(col("product_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("cust_id").isNotNull())
        .filter(col("region").isNotNull())
        .filter(col("amount").isNotNull())
        .filter(col("quantity").isNotNull())
        .filter(col("product_id") != "")
        # .withColumn("order_id", col("order_id").cast("int"))
        # .withColumn("cust_id", col("cust_id").cast("int"))
        # .withColumn("quantity", col("quantity").cast("int"))
        # .withColumn("amount", col("amount").cast("double"))
        # it is unable to cast "XYZ" to int, so we need to use try_cast
        #
        .withColumn("order_id", expr("try_cast(order_id as int)"))
        .withColumn("cust_id", expr("try_cast(cust_id as int)"))
        .withColumn("quantity", expr("try_cast(quantity as int)"))
        .withColumn("amount", expr("try_cast(amount as double)"))
        .withColumnRenamed("raw_load_time", "load_Time")
        .filter(col("amount") > 0)
        .filter(col("quantity") > 0)
        .withColumnRenamed("cust_id", "customer_id")
        .withColumnRenamed("job_id", "raw_job_id")
        .withColumn("silver_load_Time",current_timestamp())
        .withColumn("silver_job_id",lit(job_id_silver))
)

# COMMAND ----------

regions = ["US", "UK", "IN", "AU", "CA"]
valid_orders = (valid_orders.filter(
    col("product_id").rlike(r"^P\d{3}$") &
    col("region").isin(regions))
    .withWatermark("silver_load_Time", "1 hour")
    .dropDuplicates(["order_id"])
)

# COMMAND ----------

master_product = spark.read.table("kafka_stream.default.product_master").select("product_id")

# COMMAND ----------

valid_products = (
    valid_orders
        # .filter(col("product_id") == master_product.product_id)
        .join(
            master_product, 
            on="product_id",
            how = "inner"
        )
)

# COMMAND ----------

# DBTITLE 1,Cell 7
invalid_orders = (
    silver_df
        .withColumn("amount_num", expr("try_cast(amount as double)"))
        .withColumn("quantity_num", expr("try_cast(quantity as int)"))
        .filter(
            ~(col("product_id").rlike(r"^P\d{3}$")) |
            ~(col("region").isin(regions)) |
            (col("amount_num") <= 0) |
            (col("quantity_num") <= 0) |
            col("product_id").isNull() |
            col("order_id").isNull() |
            col("amount_num").isNull() |
            col("quantity_num").isNull() |
            (col("product_id") == "")
        )
)

# COMMAND ----------

invalid_orders = (
    invalid_orders
        .join(
            master_product, 
            on="product_id",
            how = "left_anti"
        )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS kafka_stream.default.checkpoints_valid_orders;
# MAGIC CREATE VOLUME IF NOT EXISTS kafka_stream.default.checkpoints_invalid_orders;

# COMMAND ----------

# DBTITLE 1,Cell 9
valid_data = (
    valid_products
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/kafka_stream/default/checkpoints_valid_orders")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable("kafka_stream.default.orders_silver_valid")
)

# COMMAND ----------

df = spark.read.table("kafka_stream.default.orders_silver_valid").orderBy("silver_load_Time", "order_id")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists kafka_stream.default.orders_silver_valid;
# MAGIC drop table if exists kafka_stream.default.orders_silver_invalid;

# COMMAND ----------

# MAGIC %skip
# MAGIC display(dbutils.fs.ls("/Volumes/kafka_stream/default/checkpoints_valid_orders"))

# COMMAND ----------

dbutils.fs.rm(
    "/Volumes/kafka_stream/default/checkpoints_valid_orders",
    recurse=True
)

dbutils.fs.rm(
    "/Volumes/kafka_stream/default/checkpoints_invalid_orders",
    recurse=True
)

# COMMAND ----------

invalid_data = (
    invalid_orders
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/kafka_stream/default/checkpoints_invalid_orders")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable("kafka_stream.default.orders_silver_invalid")
)

# COMMAND ----------

df = spark.read.table("kafka_stream.default.orders_silver_invalid")
display(df)

# COMMAND ----------

print(valid_data.isActive)
print(valid_data.status)
print(valid_data.lastProgress)

# COMMAND ----------

valid_orders.printSchema()