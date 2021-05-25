from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("helloWorld").setMaster("local")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("data/ml-100k/u.data")
    numLines = lines.count()

    print(f"Hello world! The u.data file has {numLines} lines.")