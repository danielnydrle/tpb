from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor

# 1) predikce ceny nemovitosti pomoci rozhodovaciho stromu (bez hyperparams)

spark = SparkSession.builder.master("spark://a76fb3e6550e:7077").appName("RealEstatePriceEstimation").getOrCreate()

data = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/cv06/realestate.csv")

print("Here is our inferred schema:")
data.printSchema()
# root
#  |-- No: integer (nullable = true)
#  |-- X1 transaction date: double (nullable = true)
#  |-- X2 house age: double (nullable = true)
#  |-- X3 distance to the nearest MRT station: double (nullable = true)
#  |-- X4 number of convenience stores: integer (nullable = true)
#  |-- X5 latitude: double (nullable = true)
#  |-- X6 longitude: double (nullable = true)
#  |-- Y house price of unit area: double (nullable = true)

data = data.withColumnRenamed("X1 transaction date", "TransactionDate") \
    .withColumnRenamed("X2 house age", "HouseAge") \
    .withColumnRenamed("X3 distance to the nearest MRT station", "DistanceToMRT") \
    .withColumnRenamed("X4 number of convenience stores", "NumberConvenienceStores") \
    .withColumnRenamed("X5 latitude", "Latitude") \
    .withColumnRenamed("X6 longitude", "Longitude") \
    .withColumnRenamed("Y house price of unit area", "PriceOfUnitArea")

data = data.select("HouseAge", "DistanceToMRT", "NumberConvenienceStores", "PriceOfUnitArea")

assembler = VectorAssembler(inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"], outputCol="features")
data = assembler.transform(data)

train, test = data.randomSplit([0.9, 0.1])
dt = DecisionTreeRegressor(featuresCol="features", labelCol="PriceOfUnitArea")

model = dt.fit(train)
predictions = model.transform(test)

feat_pred_selected = predictions.select("PriceOfUnitArea", "prediction")

for row in feat_pred_selected.collect():
    print((row["prediction"], row["PriceOfUnitArea"]))