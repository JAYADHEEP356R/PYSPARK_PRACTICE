from pyspark.sql.types import *

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

df_good = df_raw.subtract(df_bad).dropDuplicates(["order_id"]).dropna(subset=["order_id","price","quantity","order_date","region"]).cache()

df_good.show()

#DATA CLEANING
df_cleaned = df_good.withColumn("order_date",to_date(col("order_date"),"yyyy-MM-DD")) \
    .withColumn("price",df_good["price"].cast(DoubleType())) \
    .withColumn("quantity",df_good["quantity"].cast(IntegerType())) \
    .withColumn("product_name",trim(col("product_name"))) \
    .withColumn("category",trim(col("category"))).cache()



print("the cleaned data : ")

df_cleaned.show()

#DATA TRANSFORMATION
df_transformed = df_cleaned.withColumn("total", col("price")* col("quantity")) \
    .withColumn("year",year(col("order_date"))) \
    .withColumn("month",month(col("order_date"))) \
    .withColumn("day",day(col("order_date"))) \
    .cache()

df_transformed.show()









