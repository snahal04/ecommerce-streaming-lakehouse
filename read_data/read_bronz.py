from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("bronze")

df.orderBy("offset", ascending=False).show(20, False)
df.printSchema()
