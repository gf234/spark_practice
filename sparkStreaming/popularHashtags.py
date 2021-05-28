from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("sparkSQL") \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "twitter_tweets") \
        .load()
    keyVal = df.selectExpr("CAST(value AS STRING)", "timestamp")

    # json 에서 컬럼 추출하는 다른 방법이 있나??
    pattern = r'"text":"(.+)"'
    text = keyVal.select(
        regexp_extract("value", pattern, 1).alias("text"), "timestamp")
    # text 에서 해시테그만 추출하는 다른 방법이 있나??
    pattern2 = r'#(\w*[0-9a-zA-Z]+\w*[0-9a-zA-Z])'
    hashtags = text.select(
        regexp_extract("text", pattern2, 1).alias("hashtag"),
        "timestamp").filter('hashtag != ""')
    # window 이용
    hashtagCounts = hashtags.groupBy(
        window("timestamp", "10 seconds", "10 seconds"), "hashtag").count()
    # window 와 count 로 정렬
    sortedCounts = hashtagCounts.sort(hashtagCounts.window.desc(),
                                      hashtagCounts['count'].desc())
    # 콘솔에 출력
    query = sortedCounts.writeStream.outputMode("complete").format(
        "console").start()
    query.awaitTermination()

    spark.stop()