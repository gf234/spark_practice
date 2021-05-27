from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
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

    movieNameSchema = StructType([
        StructField("movieID", IntegerType(), True),
        StructField("movieTitle", StringType(), True),
    ])
    movieNames = spark.read.csv("data/ml-100k/u.item",
                                schema=movieNameSchema,
                                sep='|')

    movieCounts = ratings.groupBy("movieID").count().sort("count")

    mn = movieNames.alias('mn')
    mc = movieCounts.alias('mc')
    movieWithNames = mc.join(mn, mc.movieID == mn.movieID).select(
        mn.movieTitle, mc['count'])

    sortedMovieWithNames = movieWithNames.sort("count", ascending=False)

    sortedMovieWithNames.select("movieTitle", "count").show()

    spark.stop()