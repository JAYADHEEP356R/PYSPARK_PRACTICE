from OS.Initialize import doinit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


doinit()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date

Spark = SparkSession.builder.appName("data_processing").getOrCreate()

df_raw = Spark.read.option("header", True).option("inferSchema", True).csv("sales_data_dirty.csv")

# DATA VALIDATION
df_bad = df_raw.filter(
    (col("price") <= 0) |
    (col("quantity") <= 0) |
    (col("order_date") > current_date()) |
    (col("price").isNull()) |
    (col("quantity").isNull())
)

df_good = df_raw.subtract(df_bad).dropDuplicates(["order_id"]).dropna().cache()

df_good.show()












