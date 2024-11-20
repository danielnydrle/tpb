from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://a76fb3e6550e:7077").appName("MostObscureSuperheroes").getOrCreate()

sc = spark.sparkContext

graph = sc.textFile("/files/cv05/marvel-graph.txt")
names = sc.textFile("/files/cv05/marvel-names.txt")

# 3) superhrdinove, kteri maji nejmene propojeni
# predpoklad 1 propojeni jako minima

connections = graph.map(lambda x: (
    int(x.split()[0]),
    len(x.split()) - 1 if len(x.split()) > 1 else 1)
).toDF(["id", "connections"])

names = names.map(lambda x: (
    int(x.split()[0]),
    x.split("\"")[1])
).toDF(["id", "name"])

min_connections = connections.agg({"connections": "min"}).collect()[0][0]
obscure_heroes_with_min = connections.filter(connections.connections == min_connections)

result_with_min = obscure_heroes_with_min.join(names, "id").select("name").orderBy("name")
print(result_with_min.count())
result_with_min.show()

# 3 bonus) reseni bez predpokladu minimalnich propojeni
obscure_heroes_wo_min = connections.filter(connections.connections == 0)

result_wo_min = obscure_heroes_wo_min.join(names, "id").select("name").orderBy("name")
print(result_wo_min.count())
result_wo_min.show()