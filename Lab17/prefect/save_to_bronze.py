from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, TimestampType
from delta import *

def save_to_bronze():
    builder = SparkSession.builder \
        .appName("SaveToBronze") \
        .master("spark://spark:7077") \
        .config("spark.sql.warehouse.dir", "/opt/prefect/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()

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

    table_name = "delta_bronze"

    DeltaTable.createIfNotExists(spark) \
        .tableName(table_name) \
        .property("delta.enableChangeDataFeed", "true") \
        .addColumns(schema) \
        .execute()

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "demo-missions-topic") \
        .option("startingOffsets", "earliest") \
        .load()

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    query = df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints_bronze") \
        .trigger(once=True) \
        .toTable(table_name)

    query.awaitTermination()

if __name__ == "__main__":
    save_to_bronze()
