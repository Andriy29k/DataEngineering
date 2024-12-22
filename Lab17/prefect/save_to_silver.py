from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, explode, unix_timestamp, expr
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, StringType, ArrayType
from delta import *

def save_to_silver():
    builder = SparkSession.builder \
        .appName("SaveToSilver") \
        .master("spark://spark:7077") \
        .config("spark.sql.warehouse.dir", "/opt/prefect/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    schema = StructType([
        StructField("mission_name", StringType()),
        StructField("failures", ArrayType(StructType([
            StructField("time", FloatType()),
            StructField("altitude", FloatType()),
            StructField("reason", StringType())
        ]))),
        StructField("date_utc", TimestampType()),
        StructField("rocket", StringType())
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

    # Обробка даних із Bronze таблиці
    silver_df = bronze_df \
        .filter(col("_change_type") == "insert") \
        .withColumn("failure_detail", explode(col("failures"))) \
        .withColumn("failure_time", (unix_timestamp(col("date_utc")) + col("failure_detail.time")).cast(TimestampType())) \
        .withColumn("failure_reason", trim(col("failure_detail.reason"))) \
        .withColumn("failure_altitude", col("failure_detail.altitude").cast(FloatType())) \
        .withColumn("launch_date", col("date_utc").cast(TimestampType())) \
        .select("mission_name", "rocket", "failure_time", "failure_reason", "failure_altitude", "launch_date", "failures", "date_utc")

    # Фільтрація та видалення дублікатів
    silver_df = silver_df \
        .filter(col("mission_name").isNotNull()) \
        .filter(col("failure_reason").isNotNull()) \
        .filter(col("failure_time").isNotNull()) \
        .filter(col("failure_altitude").isNotNull()) \
        .dropDuplicates(["mission_name", "failure_time", "failure_reason"])

    query = silver_df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints_silver") \
        .option("mergeSchema", "true") \
        .trigger(once=True) \
        .toTable(table_name)

    query.awaitTermination()

if __name__ == "__main__":
    save_to_silver()
