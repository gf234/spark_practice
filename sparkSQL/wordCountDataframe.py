from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("sparkSQL") \
        .getOrCreate()

    lines = spark.read.text("data/book.txt")

    words = lines.select(explode(split(
        lines.value, r'\W+')).alias("word")).filter('word != ""')

    lowerCaseWords = words.select(lower(words.word).alias("word"))

    wordCounts = lowerCaseWords.groupBy("word").count().sort("count",
                                                             ascending=False)
    wordCounts.show()

    spark.stop()