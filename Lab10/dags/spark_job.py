from pyspark.sql import SparkSession 
from pyspark.sql.functions import avg 
 
spark = SparkSession.builder.appName("Airflow Spark Job").getOrCreate() 
 
data = [('Joe Dou', 'Social networks', 120), 
        ('Masha Pupkina', 'Games', 85),
        ('Alice Smith', 'Social networks', 90),
        ('Ivan Ivanov', 'Productivity', 60),
        ('Donatello', 'Games', 110),
        ('Raphael', 'Finances', 25),
        ('Splinter', 'Shoping', 50),
        ('Mickianjelo', 'Sport', 61)
] 
df = spark.createDataFrame(data, ["Name", "Application Type", "Use Count"])  
 
df.show(truncate=False) 
 
df.groupBy("Application Type").agg(avg("Use Count").alias("Average Usage Count")).show(truncate=False) 
 
spark.stop()