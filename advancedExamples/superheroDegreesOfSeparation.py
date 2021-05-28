from pyspark import SparkConf, SparkContext


def parseGraph(line):
    '''
    노드 형태로 변환
    '''
    heroID, *connections = line.split()
    heroID = int(heroID)
    connections = list(map(int, connections))

    distance = 9999
    color = "WHITE"

    if heroID == startCharacterID:
        distance = 0
        color = "GRAY"

    return (heroID, (connections, distance, color))


def bfsMap(node):
    '''
    GRAY 인 노드 처리
    '''
    results = []
    heroID = node[0]
    connections, distance, color = node[1]

    if color == "GRAY":
        for connection in connections:
            newID = connection
            newDist = distance + 1
            newColor = "GRAY"

            if endCharacterID == newID:
                hitCounter.add(1)

            newNode = (newID, ([], newDist, newColor))
            results.append(newNode)
        color = "BLACK"

    results.append((heroID, (connections, distance, color)))
    return results


def bfsReduce(x, y):
    '''
    노드 하나만 남기기
    '''
    con1, d1, c1 = x
    con2, d2, c2 = y

    connections = con1 + con2
    distance = min(d1, d2)
    color = "WHITE"
    if "BLACK" in (c1, c2):
        color = "BLACK"
    elif "GRAY" in (c1, c2):
        color = "GRAY"

    return (connections, distance, color)


if __name__ == "__main__":
    startCharacterID = 5306
    endCharacterID = 14

    conf = SparkConf().setAppName("helloWorld")
    sc = SparkContext('local', conf=conf)

    # 도착한것 판단하기 위해 설정
    hitCounter = sc.accumulator(0)

    lines = sc.textFile("data/Marvel-graph.txt")
    nodes = lines.map(parseGraph)

    for i in range(1, 11):
        print(f"Running BFS Iteration# {i}")

        mapped = nodes.flatMap(bfsMap)
        print(f"Processing {mapped.count()} values.")

        if hitCounter.value > 0:
            print(
                f"Hit the target character! From {hitCounter.value} different direction(s)."
            )
            break

        nodes = mapped.reduceByKey(bfsReduce)