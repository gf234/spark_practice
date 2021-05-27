from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("sparkSQL") \
        .getOrCreate()

    temperatureSchema = StructType(
        [
            StructField("stationID", StringType(), True),
            StructField("date", IntegerType(), True),
            StructField("measure_type", StringType(), True),
            StructField("temperature", FloatType(), True)
        ]
    )

    df = spark.read.csv("data/1800.csv", schema=temperatureSchema)

    minTemps = df.filter('measure_type = "TMIN"')

    stationTemps = minTemps.select("stationID", round((minTemps.temperature / 10), 2).alias("temperature"))

    minTempByStation = stationTemps.groupBy("stationID").min("temperature")

    minTempByStation.show()

    spark.stop()