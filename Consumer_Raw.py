# Databricks notebook source
# MAGIC %md
# MAGIC databricks secrets create-scope kafka-scope
# MAGIC
# MAGIC databricks secrets put-secret kafka-scope api-key
# MAGIC
# MAGIC databricks secrets put-secret kafka-scope api-secret
# MAGIC

# COMMAND ----------

api_key = dbutils.secrets.get(
    scope="kafka-scope",
    key="api-key"
)
# print(api_key)

api_secret = dbutils.secrets.get(
    scope="kafka-scope",
    key="api-secret"
)
# print(api_secret)

# COMMAND ----------

df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",
                "pkc-921jm.us-east-2.aws.confluent.cloud:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        # .option("checkpointLocation", "/Volumes/kafka_stream/default/checkpoints")  No meaning to add here add while writing
        .option("failOnDataLoss", "false")
        # .option("outputMode","append")

        # Confluent Cloud authentication
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{api_key}" '
            f'password="{api_secret}";'
        )

        .load()
)

# COMMAND ----------

# from confluent_kafka import Consumer

# conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
#         'security.protocol': 'SASL_SSL',
#         'sasl.mechanism': 'PLAIN',
#         'sasl.username': api_key,
#         'sasl.password': api_secret,
#         'group.id': 'foo',
#         'auto.offset.reset': 'smallest'}

# consumer = Consumer(conf)

# COMMAND ----------

job_id = dbutils.widgets.get("job_id")
print(job_id)

# COMMAND ----------

from pyspark.sql.functions import col, cast, current_timestamp, lit
from pyspark.sql import functions as F

readable_df =( df.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
    )
    .withColumn("load_Time", F.date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("job_id", lit(job_id))
)

# COMMAND ----------

read_df = (
    readable_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/kafka_stream/default/checkpoints")
    .trigger(availableNow=True)
    .option("mergeSchema","True")
    .table("kafka_stream.default.orders_raw")
)

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC %sql
# MAGIC SELECT key, value, topic, partition, offset, timestamp, load_Time, job_id 
# MAGIC FROM kafka_stream.default.orders_raw
# MAGIC ORDER BY load_Time DESC, offset DESC
# MAGIC LIMIT 50

# COMMAND ----------

spark.read.table("kafka_stream.default.orders_raw").printSchema()

# COMMAND ----------

print("sucessful")

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC ALTER TABLE kafka_stream.default.orders_raw
# MAGIC ALTER COLUMN job_id TYPE STRING;

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC drop TABLE if exists kafka_stream.default.orders_raw;

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.fs.rm(
# MAGIC     "/Volumes/kafka_stream/default/checkpoints",
# MAGIC     recurse=True
# MAGIC )