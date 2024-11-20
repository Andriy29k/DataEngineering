from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OnlineTransactionsMedallion") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

data = [
    ("2024-11-01", "Client1", 500.0, "Credit Card"),
    ("2024-11-01", "Client2", 15000.0, "Bank Transfer"),
    ("2024-11-02", "Client1", 0.89, "PayPal"),
    ("2024-11-02", "Client3", 700.0, "Credit Card"),
    ("2024-11-02", "Client3", 700.0, "Credit Card"),  
    ("2024-11-18", "Client4", 9000.0, "Bank Transfer"),
    ("2024-11-18", "Client4", 500.0, "Credit Card"),
    ("2024-11-01", "Client2", 15000.0, "Bank Transfer"),
    ("2024-11-02", "Client1", 50.0, "PayPal"),
    ("2024-11-02", "Client1", 500.0, "Credit Card"),
    ("2024-11-02", "Client1", 500.0, "Credit Card"),  
    ("2024-11-03", "Client5", 35000.0, "Bank Transfer"),
]
columns = ["date", "client", "amount", "payment_method"]

bronze_df = spark.createDataFrame(data, columns)
bronze_df.show()

bronze_df.write.format("delta").mode("overwrite").save("/home/jovyan/work/bronze")

bronze_data = spark.read.format("delta").load("/home/jovyan/work/bronze")

filtered_data = bronze_data.filter((bronze_data["amount"] >= 1) & (bronze_data["amount"] <= 10000))

unique_data = filtered_data.dropDuplicates()

from pyspark.sql.functions import when

silver_df = unique_data.withColumn(
    "payment_category",
    when(unique_data["payment_method"] == "Credit Card", "Card")
    .when(unique_data["payment_method"] == "Bank Transfer", "Bank")
    .when(unique_data["payment_method"] == "PayPal", "Digital Wallet")
    .otherwise("Other")
)

silver_df.show()
silver_df.write.format("delta").mode("overwrite").save("/home/jovyan/work/silver")

from pyspark.sql.functions import avg, count

silver_data = spark.read.format("delta").load("/home/jovyan/work/silver")

avg_transactions = silver_data.groupBy("client").agg(
    count("amount").alias("transaction_count"),
    avg("amount").alias("avg_transaction_amount")
)
print("Average transactions count per client")
avg_transactions.show()

payment_popularity = silver_data.groupBy("payment_category").agg(
    count("payment_method").alias("method_count")
)
print("Popularity of payment methods")
payment_popularity.show()

avg_transactions.write.format("delta").mode("overwrite").save("/home/jovyan/work/gold_avg")
payment_popularity.write.format("delta").mode("overwrite").save("/home/jovyan/work/gold_popularity")

