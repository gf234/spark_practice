from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext


conf = SparkConf() \
    .setMaster("local[*]") \
    .setAppName("WordCount")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 15)

lines = ssc.socketTextStream("localhost", 9999)

words = lines.flatMap(lambda line: line.split())

pairs = words.map(lambda word: (word, 1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

sortedWordCounts = wordCounts.transform(
    lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

sortedWordCounts.pprint()

ssc.start()
ssc.awaitTermination()
