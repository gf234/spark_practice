from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("helloWorld").setMaster("local")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("data/book.txt")
    # 대소문자 구분 없이 같은 단어로 하도록 lower case 로 바꿔줌
    lowerCaselines = lines.map(lambda x : x.lower())
    # 단어로 나눠주고 count 하기 위해 (x, 1) 형식으로 변형
    words = lowerCaselines.flatMap(lambda x : x.split()).map(lambda x : (x, 1))

    counts = words.reduceByKey(lambda x, y : x + y)
    # count 를 기준으로 정렬하기 위해 뒤집어 준다.
    flippedCounts = counts.map(lambda x : (x[1], x[0]))

    sortedCounts = flippedCounts.sortByKey()

    results = sortedCounts.collect()

    for count, word in results:
        print(f'{word} : {count}')
