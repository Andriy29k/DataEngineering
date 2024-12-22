from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count
from delta import *

def save_to_gold():
    # Ініціалізація SparkSession
    builder = SparkSession.builder \
        .appName("SaveToGold") \
        .master("spark://spark:7077") \
        .config("spark.sql.warehouse.dir", "/opt/prefect/spark-warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Читання даних із Silver таблиці
    silver_df = spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .load("/opt/prefect/spark-warehouse/delta_silver")

    # Обробка даних:
    # 1. Підрахунок кількості невдалих запусків за причинами відмов.
    # 2. Розрахунок середньої висоти на момент відмови.
    gold_df = silver_df.filter(col("_change_type") == "insert") \
        .groupBy("failure_reason") \
        .agg(
            count("*").alias("failure_count"),
            avg("failure_altitude").alias("average_failure_altitude")
        )

    # Запис у Gold таблицю Delta
    query = gold_df.writeStream.format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/checkpoints_gold") \
        .trigger(once=True) \
        .toTable("delta_gold")

    query.awaitTermination()

    # Експорт таблиці в CSV для аналітики
    parquet_df = spark.read.parquet("/opt/prefect/spark-warehouse/delta_gold")
    parquet_df.toPandas().to_csv('/opt/prefect/spark-warehouse/result/spacex_failure_analysis.csv')

if __name__ == "__main__":
    save_to_gold()
