from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BronzeLayer")
    .getOrCreate()
)

print(spark.version)

spark.stop()