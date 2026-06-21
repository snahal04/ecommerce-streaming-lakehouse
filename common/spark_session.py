# common/spark_session.py
from pyspark.sql import SparkSession

def get_spark_session(app_name: str, delta_version: str = "3.1.0") -> SparkSession:
    """
    Returns a SparkSession pre-configured with Delta Lake extensions 
    and packages for local Docker development.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        # Automatically pulls down the Delta JAR inside your Docker container
        .config("spark.jars.packages", f"io.delta:delta-spark_2.12:{delta_version}")
        # Teaches Spark how to read/write Delta format and SQL commands (MERGE, UPDATE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Configures the internal catalog to handle Delta metadata tables safely
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )