from pyspark.sql import SparkSession
from delta import *

builder = SparkSession.builder.appName("Energy Monitoring with Delta Lake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("building_id", IntegerType(), nullable=False),
    StructField("region", StringType(), nullable=False),
    StructField("month", StringType(), nullable=False),
    StructField("energy_consumption", DoubleType(), nullable=False),
    StructField("energy_cost", DoubleType(), nullable=False)
])

data = [
    (1, "North", "2024-09", 1500.0, 300.0),
    (2, "East", "2024-09", 1200.0, 240.0),
    (3, "South", "2024-09", 1800.0, 360.0),
    (4, "West", "2024-09", 900.0, 540.0)
]

df = spark.createDataFrame(data, schema=schema)
df.show()

DeltaTable.createIfNotExists(spark) \
    .tableName("energy_consumption") \
    .property("delta.enableChangeDataFeed", "true") \
    .addColumns(schema) \
    .execute()

df.write.format("delta").mode("overwrite").saveAsTable("energy_consumption")

new_data = [
    (1, "North", "2024-10", 1600.0, 320.0),  
    (5, "West", "2024-10", 950.0, 280.0)   
]

new_df = spark.createDataFrame(new_data, schema=schema)
new_df.show()

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "energy_consumption")

delta_table.alias("old_data") \
    .merge(
        new_df.alias("new_data"),
        "old_data.building_id = new_data.building_id AND old_data.month = new_data.month"
    ) \
    .whenMatchedUpdate(set={
        "energy_consumption": "new_data.energy_consumption",
        "energy_cost": "new_data.energy_cost"
    }) \
    .whenNotMatchedInsert(values={
        "building_id": "new_data.building_id",
        "region": "new_data.region",
        "month": "new_data.month",
        "energy_consumption": "new_data.energy_consumption",
        "energy_cost": "new_data.energy_cost"
    }) \
    .execute()

delta_table.toDF().show()
delta_table.history().show()

previous_version_df = spark.read.format("delta").option("versionAsOf", 1).table("energy_consumption")
previous_version_df.show()

delta_table.restoreToVersion(0)
delta_table.toDF().show()

delta_table.optimize().executeCompaction()

delta_table.vacuum(retentionHours=168)  # 7 days

cdf = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", "0") \
    .table("energy_consumption")

cdf.show()

spark.stop()


