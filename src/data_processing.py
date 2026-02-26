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

df_good = df_raw.subtract(df_bad).dropDuplicates(["order_id"]).dropna(subset=["order_id","price","quantity","order_date"]).cache()

df_good.show()

#DATA CLEANING
df_cleaned = df_good.withColumn("order_date",to_date(col("order_date"),"YYYY-MM-DD")) \
    .withColumn("price",df_good["price"].cast(DoubleType())) \
    .withColumn("quantity",df_good["quantity"].cast(IntegerType())) \
    .cache()



print("the cleaned data : ")

df_cleaned.show()









