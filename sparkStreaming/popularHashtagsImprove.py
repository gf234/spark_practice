import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# packages 옵션 추가하는 방법
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

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
        .option("startingOffsets", "earliest") \
        .load()
    valTime = df.selectExpr("CAST(value AS STRING)", "timestamp")

    # json 이용해서 schema 추출
    schema = schema_of_json(r'''{
  "entities": {
    "hashtags": [      
      {
        "text": "string"
      }
    ]
  }
}
''')
    # json 데이터 읽기
    jsonData = valTime.select("timestamp",
                              from_json("value", schema).alias("json_data"))

    # 해시태그 텍스트 추출
    hashtagList = jsonData.select(
        "timestamp",
        jsonData.json_data.entities.hashtags.text.alias("hashtag_list"))
    # 해시태그 분리
    hashtags = hashtagList.select("timestamp",
                                  explode("hashtag_list").alias("hashtag"))

    # window 이용
    hashtagCounts = hashtags.groupBy("hashtag").count()
    # window 와 count 로 정렬
    sortedCounts = hashtagCounts.sort(hashtagCounts['count'].desc()).limit(10)
    # 콘솔에 출력
    query = sortedCounts.writeStream.outputMode("complete").format(
        "console").start()
    query.awaitTermination()

    spark.stop()