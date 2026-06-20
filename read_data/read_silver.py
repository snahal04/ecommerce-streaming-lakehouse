from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("silver")

df.show(10, truncate=False)
df.printSchema()