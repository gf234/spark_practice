from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("sparkSQL") \
        .getOrCreate()

    people = spark.read.csv("data/fakefriends.csv", header=True)

    print("Here is our inferred schema:")
    people.printSchema()

    print("Let's select the name column:")
    people.select("name").show()

    print("Filter out anyone over 21:")
    people.filter(people.age > 21).show()

    print("Group by age:")
    people.groupBy("age").count().show()

    print("Make everyone 10 years older:")
    people.select(people.name, (people.age + 10)).show()

    spark.stop()