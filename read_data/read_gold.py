from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("gold/product_metrics")

df.show(truncate=False)
df.printSchema()