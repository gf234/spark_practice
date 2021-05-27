from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("sparkSQL") \
        .getOrCreate()

    orderSchema = StructType([
        StructField("customerID", IntegerType(), True),
        StructField("itemID", IntegerType(), True),
        StructField("cost", FloatType(), True),
    ])

    df = spark.read.csv("data/customer-orders.csv", schema=orderSchema)

    customerSpend = df.select("customerID", "cost")

    totalSpent = customerSpend.groupBy("customerID").agg(
        round(sum("cost"), 2).alias("total_spent")).sort("total_spent",
                                                         ascending=False)

    totalSpent.show()

    spark.stop()