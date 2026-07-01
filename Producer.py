# Databricks notebook source
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

data = dbutils.jobs.taskValues.get(
        taskKey="Generating_30_Random_Events",
        key="orders_data",
        default=["No Data"],
        debugValue=["Testing"]
    )

# COMMAND ----------

display(len(data))
display(data)

# COMMAND ----------

# MAGIC %skip
# MAGIC # (
# MAGIC #     df.selectExpr(
# MAGIC #         "CAST(key AS STRING) AS key",
# MAGIC #         "CAST(value AS STRING) AS value"
# MAGIC #     )
# MAGIC #     .write
# MAGIC #     .format("kafka")
# MAGIC #     .option(
# MAGIC #         "kafka.bootstrap.servers",
# MAGIC #         "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
# MAGIC #     )
# MAGIC #     .option("topic", "orders")
# MAGIC #     .option("kafka.security.protocol", "SASL_SSL")
# MAGIC #     .option("kafka.sasl.mechanism", "PLAIN")
# MAGIC #     .option(
# MAGIC #         "kafka.sasl.jaas.config",
# MAGIC #         f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
# MAGIC #         f'username="{api_key}" '
# MAGIC #         f'password="{api_secret}";'
# MAGIC #     )
# MAGIC #     .save()
# MAGIC # )

# COMMAND ----------

# Reference
# https://docs.confluent.io/kafka-clients/python/current/overview.html

from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'client.id': socket.gethostname()}

producer = Producer(conf)

# COMMAND ----------

topic_name = dbutils.widgets.get("topic_name")
print(topic_name)

# COMMAND ----------

import json
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

for record in data:
    producer.produce(topic=topic_name, key=record["order_id"], value=json.dumps(record), callback=acked)

    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    producer.poll(0)
producer.flush()

# COMMAND ----------

# MAGIC %skip
# MAGIC import json
# MAGIC topic=dbutils.widgets.get("topic_name")
# MAGIC display(topic)
# MAGIC for record in data:
# MAGIC     key=record["order_id"], value=json.dumps(record)
# MAGIC     display(key)
# MAGIC     display(value)
# MAGIC
# MAGIC

# COMMAND ----------

print("Success")