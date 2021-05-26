from ast import parse
from pyspark import SparkContext, SparkConf


def parseLine(line):
    fields = line.split(',')
    userID = int(fields[0])
    cost = float(fields[2])
    return (userID, cost)


if __name__ == "__main__":
    conf = SparkConf().setAppName("helloWorld").setMaster("local")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("data/customer-orders.csv")

    userCosts = lines.map(parseLine)

    totalSpentByCustomer = userCosts.reduceByKey(lambda x, y: x + y)

    flipped = totalSpentByCustomer.map(lambda x: (x[1], x[0]))
    sortedTotal = flipped.sortByKey()

    results = sortedTotal.collect()

    for total, userID in results:
        print(f'{userID} : {total:0.2f}$')