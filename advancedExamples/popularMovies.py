from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def loadMovieNames():
    movieNames = dict()
    with open("data/ml-100k/u.item", 'r', encoding='ISO-8859-1') as f:
        lines = f.readlines()
        for line in lines:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


if __name__ == "__main__":
    movieNames = loadMovieNames()

    spark = SparkSession.builder \
        .master("local") \
        .appName("sparkSQL") \
        .getOrCreate()

    ratingSchema = StructType([
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
    ])

    ratings = spark.read.csv("data/ml-100k/u.data",
                             schema=ratingSchema,
                             sep='\t')

    movieCounts = ratings.groupBy("movieID").count().sort("count")

    results = movieCounts.collect()

    for movieID, cnt in results:
        print(f'{movieNames[movieID]} : {cnt}')

    spark.stop()