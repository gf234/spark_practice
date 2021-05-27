import re
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def parseGraph(line):
    heroID, *connections = re.split(r'\s+', line)
    return (int(heroID), len(connections))


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("sparkSQL") \
        .getOrCreate()

    # 이름 df로 읽기
    heroNameSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    names = spark.read.csv("data/Marvel-names.txt",
                           schema=heroNameSchema,
                           sep=" ")
    # 그래프 정보 RDD 로 읽기
    lines = spark.sparkContext.textFile("data/Marvel-graph.txt")
    numConnections = lines.map(parseGraph)

    totalFriendsByCharacter = numConnections.reduceByKey(lambda x, y: x + y)
    flipped = totalFriendsByCharacter.map(lambda x: (x[1], x[0]))

    minFriends = flipped.min()[0]

    mostObscureSuperheros = totalFriendsByCharacter.filter(
        lambda x: x[1] == minFriends)
    # RDD -> dataframe 변환
    heroFriendsSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("friends", IntegerType(), True)
    ])
    mostObscureDf = spark.createDataFrame(mostObscureSuperheros,
                                          schema=heroFriendsSchema)

    minConnectionWithNames = mostObscureDf.join(names,
                                                names.id == mostObscureDf.id)
    minConnectionWithNames.select("name").show()
