from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("sparkSQL") \
        .getOrCreate()

    peopleSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("friends", IntegerType(), True),
    ])

    people = spark.read.csv("data/fakefriends.csv",
                            header=True,
                            schema=peopleSchema)

    avgFriendsByAge = people.groupBy("age").agg(round(avg("friends"), 2)).alias("friends_avg").sort("age")
    avgFriendsByAge.show()

    spark.stop()