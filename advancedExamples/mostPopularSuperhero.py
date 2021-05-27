import re
from pyspark import SparkConf, SparkContext


def parseGraph(line):
    heroID, *connections = re.split(r'\s+', line)
    return (int(heroID), len(connections))


def parseNames(line):
    fields = line.split('"')
    if len(fields) > 1:
        return (int(fields[0]), fields[1])


if __name__ == "__main__":
    conf = SparkConf().setAppName("example").setMaster("local")
    sc = SparkContext(conf=conf)

    rows = sc.textFile("data/Marvel-names.txt")
    names = rows.map(parseNames)

    lines = sc.textFile("data/Marvel-graph.txt")
    numConnections = lines.map(parseGraph)

    totalFriendsByCharacter = numConnections.reduceByKey(lambda x, y: x + y)
    flipped = totalFriendsByCharacter.map(lambda x: (x[1], x[0]))

    mostPopular = flipped.max()

    mostPopularName = names.lookup(mostPopular[1])[0]

    print(
        f"{mostPopularName} is the most popular superhero with {mostPopular[0]} co-appearances."
    )
