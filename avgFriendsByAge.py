from pyspark import SparkContext, SparkConf

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


if __name__ == "__main__":
    conf = SparkConf().setAppName("helloWorld").setMaster("local")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("data/fakefriends-noheader.csv")
    
    rdd = lines.map(parseLine)
    # age 마다 사람이 총 몇명인지 구하기 위해 (x, 1) 형태로 변형한 후 reduce
    totalByAge = rdd.mapValues(lambda x : (x, 1)).reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1]))
    # 나눠서 평균 구하고 나이순으로 정렬
    avgByAge = totalByAge.mapValues(lambda x : x[0]/x[1]).sortByKey()

    results = avgByAge.collect()

    for result in results:
        age = result[0]
        avgNumFriends = result[1]
        print(f"age = {age}, avgNumFriends = {avgNumFriends:0.2f}")
