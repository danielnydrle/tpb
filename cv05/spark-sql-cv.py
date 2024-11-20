from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://a76fb3e6550e:7077").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/cv05/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()
# root
#  |-- userID: integer (nullable = true)
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- friends: integer (nullable = true)

# 1) prumerny pocet kamaradu dle veku
(people
    .groupBy("age")
    .avg("friends")
    .orderBy("age")
    .show())