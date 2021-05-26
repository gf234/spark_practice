from pyspark import SparkContext, SparkConf

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) / 10
    return (stationID, entryType, temperature)


if __name__ == "__main__":
    conf = SparkConf().setAppName("helloWorld").setMaster("local")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("data/1800.csv")
    
    rdd = lines.map(parseLine)

    minTemps = rdd.filter(lambda x : x[1] == 'TMIN')

    stationTemps = minTemps.map(lambda x : (x[0], x[2]))

    minStationTemps = stationTemps.reduceByKey(lambda x, y : min(x, y))
    
    results = minStationTemps.collect()

    for station, temp in results:
        print(f'{station} minimum temperature: {temp:0.2f}')
