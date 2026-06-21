from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("bronze")

df.show(truncate=False)
df.printSchema()
