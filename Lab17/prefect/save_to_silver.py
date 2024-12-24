from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, unix_timestamp, trim
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, StringType
from delta import *

def save_to_silver():
    builder = SparkSession.builder \
        .appName("SaveToSilver") \
        .master("spark://spark:7077") \
        .config("spark.sql.warehouse.dir", "/opt/prefect/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    schema = StructType([
        StructField("mission_name", StringType()),
        StructField("rocket", StringType()),
        StructField("failure_time", TimestampType()),
        StructField("failure_reason", StringType()),
        StructField("failure_altitude", FloatType()),
        StructField("launch_date", TimestampType())
    ])

    table_name = "delta_silver"

    DeltaTable.createIfNotExists(spark) \
        .tableName(table_name) \
        .property("delta.enableChangeDataFeed", "true") \
        .addColumns(schema) \
        .execute()

    bronze_df = spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .load("/opt/prefect/spark-warehouse/delta_bronze")

    silver_df = bronze_df \
        .filter(col("_change_type") == "insert") \
        .withColumn("failure_detail", explode(col("failures"))) \
        .withColumn("failure_time", (unix_timestamp(col("date_utc")) + col("failure_detail.time")).cast("timestamp")) \
        .withColumn("failure_reason", trim(col("failure_detail.reason"))) \
        .withColumn("failure_altitude", col("failure_detail.altitude").cast("float")) \
        .withColumn("launch_date", col("date_utc").cast("timestamp")) \
        .select("mission_name", "rocket", "failure_time", "failure_reason", "failure_altitude", "launch_date") \
        .filter(col("mission_name").isNotNull() & col("failure_reason").isNotNull() & col("failure_time").isNotNull() & col("failure_altitude").isNotNull()) \
        .dropDuplicates(["mission_name", "failure_time", "failure_reason"])

    query = silver_df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints_silver") \
        .trigger(once=True) \
        .toTable(table_name)

    query.awaitTermination()

if __name__ == "__main__":
    save_to_silver()
