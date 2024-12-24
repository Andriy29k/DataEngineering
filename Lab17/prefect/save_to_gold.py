from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col
from delta import *

def save_to_gold():
    builder = SparkSession.builder \
        .appName("SaveToGold") \
        .master("spark://spark:7077") \
        .config("spark.sql.warehouse.dir", "/opt/prefect/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    silver_df = spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .load("/opt/prefect/spark-warehouse/delta_silver")

    gold_df = silver_df \
        .filter(col("_change_type") == "insert") \
        .groupBy("failure_reason", "launch_date").agg(
            count("*").alias("failure_count"),
            avg("failure_altitude").alias("average_failure_altitude")
        )

    query = gold_df.writeStream.format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/checkpoints_gold") \
        .trigger(once=True) \
        .toTable("delta_gold")

    query.awaitTermination()

    parquet_df = spark.read.parquet("/opt/prefect/spark-warehouse/delta_gold")
    parquet_df.toPandas().to_csv('/opt/prefect/spark-warehouse/result/spacex_failure_analysis.csv', index=False)

if __name__ == "__main__":
    save_to_gold()
