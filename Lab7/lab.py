from pyspark.sql import SparkSession
from delta import *

builder = SparkSession.builder.appName("Bank transactions") \
 .config("spark.sql.extensions",
"io.delta.sql.DeltaSparkSessionExtension") \
 .config("spark.sql.catalog.spark_catalog",
"org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("date", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=False)  
])

data = [
    (1, "2024-10-01", 1500.00, "deposit"),
    (2, "2024-10-02", -1000.00, "withdrawal"),
    (3, "2024-10-03", 2000.00, "deposit"),
    (4, "2024-10-04", 1500.00, "deposit"),
    (5, "2024-10-05", -950.00, "withdrawal"),
    (6, "2024-10-06", 225.00, "deposit")
]

df = spark.createDataFrame(data, schema=schema)
df.show()

df.write.format("delta").mode("append").save("/tmp/delta-table")

delta_table = spark.read.format("delta").load("/tmp/delta-table")
delta_table.show()
delta_table.printSchema()

new_data = [
    (7, "2024-10-04", 1000.00, "deposit"),
    (8, "2024-10-05", -200.00, "withdrawal"),
    (9, "2024-10-04", 1006.00, "deposit"),
    (10, "2024-10-05", -999.00, "withdrawal")
]
new_df = spark.createDataFrame(new_data, schema=schema)
new_df.write.format("delta").mode("append").save("/tmp/delta-table")

updated_table = spark.read.format("delta").load("/tmp/delta-table")
updated_table.show()

incorrect_data = [
    (9, "2024-10-09", None, "deposit")
]

incorrect_df = spark.createDataFrame(incorrect_data, schema=schema)
incorrect_df.write.format("delta").mode("append").save("/tmp/delta-table")

previous_version = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
previous_version.show()