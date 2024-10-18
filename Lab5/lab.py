from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Basic Spark Lab").getOrCreate()
print(f"Spark Version: {spark.version}") 

data = [("Moby Dick", "Herman Melville", 1851, 635), ("Pride and Prejudice",
"Jane Austen", 1813, 432), ("Ulysses", "James Joyce", 1922, 730)]
columns = ["Title", "Author", "Year", "Pages"]

df = spark.createDataFrame(data, schema=columns)

df.show()

df_sorted = df.orderBy(col("Pages").desc())
df_sorted.show()

oldest_book = df.orderBy(col("Year").asc()).limit(1)
oldest_book.show()

df_with_category = df.withColumn(
    "Page Category", 
    when(col("Pages") < 300, "Коротка")
    .when((col("Pages") >= 300) & (col("Pages") <= 600), "Середня")
    .otherwise("Довга")
)

df_with_category.show()
