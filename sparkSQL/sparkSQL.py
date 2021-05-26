from email import header
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("sparkSQL") \
        .getOrCreate()

    people = spark.read.csv("data/fakefriends.csv", header=True)

    people.printSchema()

    people.createOrReplaceTempView("people")

    teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    results = teenagers.collect()

    for result in results:
        print(result)

    spark.stop()