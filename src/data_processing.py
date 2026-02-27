from pyspark.sql.types import *
from OS.Initialize import doinit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date
from pyspark.sql.window import Window
doinit()

Spark = SparkSession.builder.appName("data_processing").getOrCreate()
df_raw = Spark.read.option("header", True).option("inferSchema", True).csv("sales_data_dirty.csv")
print("INITIAL RAW DATA:")
df_raw.show()

#DATA CLEANING
df_cleaned = df_raw.withColumn("order_date",to_date(col("order_date"),"yyyy-MM-dd")) \
    .withColumn("price",df_raw["price"].cast(DoubleType())) \
    .withColumn("quantity",df_raw["quantity"].cast(IntegerType())) \
    .withColumn("product_name",trim(col("product_name"))) \
    .withColumn("category",trim(col("category"))) \
    .withColumn("payment_method",when(col("payment_method").isNull(),"UNKNOWN").otherwise(col("payment_method"))) \
    .withColumn("region",when(col("region").isNull(),"UNKNOWN").otherwise(col("region"))) \
    .cache()

print("the cleaned data : ")
#df_cleaned.show()

# DATA VALIDATION
df_bad = df_cleaned.filter(
    (col("price") <= 0) |
    (col("quantity") <= 0) |
    (col("order_date") > current_date()) |
    (col("price").isNull()) |
    (col("quantity").isNull())
)
df_good = df_cleaned.subtract(df_bad).dropDuplicates(["order_id"]).dropna(subset=["order_id","price","quantity","order_date","region"])

#df_good.show()

#DATA TRANSFORMATION - PHASE 1
df_transformed1 = df_good.withColumn("total_amount", col("price")* col("quantity")) \
    .withColumn("year",year(col("order_date"))) \
    .withColumn("month",month(col("order_date"))) \
    .withColumn("day",day(col("order_date"))) \
    .withColumn("year_month",concat_ws("-",col("year"),col("month"))) \
    .withColumn("quarter",quarter(col("order_date"))) \


print("FINAL PHASE 1 TRANSFORMED DATA:")
#df_transformed1.show()

#DATA TRANSFORMATION - PHASE 2
#UDF for sales_category
def sales_cat(salary):
    if salary > 100000:
        return "high"
    elif salary >=20000:
        return  "medium"
    else:
        return "low"

cat_udf = udf(sales_cat,StringType())

df_transformed2 = df_transformed1.withColumn("sales_category",cat_udf(col("total_amount"))) \
    .withColumn("bulk_flag",when(col("quantity")>=5,"YES").otherwise("NO")) \
    .withColumn("day_type", when((date_format(col("order_date"),"EEEE") == "Saturday") \
                                 | (date_format(col("order_date"),"EEEE") =="Sunday"),"WeekEnd") \
                                 .otherwise("WeekDay")) \
    .withColumn("region_type", when((col("region") == "North") | (col("region") == "South"),"Domestic").otherwise("International")) \


df_transformed2.show()





