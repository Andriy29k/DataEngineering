from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

spark = SparkSession.builder \
    .appName("TicketSalesAnalysis") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("ticket_id", IntegerType(), True),
    StructField("event_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity_sold", IntegerType(), True),
    StructField("category", StringType(), True)
])

df = spark.read.csv("/home/jovyan/work/ticket_sales.csv", header=True, schema=schema)
df.show()

from pyspark.sql.functions import sum

revenue_df = df.withColumn("total_revenue", df["price"] * df["quantity_sold"]) \
               .groupBy("category") \
               .agg(sum("total_revenue").alias("total_revenue"))
revenue_df.show()

revenue_df.write.format("delta").mode("overwrite").saveAsTable("ticket_sales_revenue")

delta_df = spark.read.format("delta").table("ticket_sales_revenue")
delta_df.show()