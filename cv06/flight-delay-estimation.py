import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.master("spark://a76fb3e6550e:7077").appName("FlightDelayEstimation").getOrCreate()

df1 = spark.read.csv("/files/cv06/1987.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/files/cv06/1989.csv", header=True, inferSchema=True)
df3 = spark.read.csv("/files/cv06/2008.csv", header=True, inferSchema=True)

df = df1.union(df2).union(df3)

df = df.filter(df["Cancelled"] == 0)

values = ["Year", "Month", "DayofMonth", "DayofWeek", "CRSDepTime", "CRSArrTime", "UniqueCarrier", "CRSElapsedTime", "Origin", "Dest", "Distance", "ArrDelay"]
for value in values:
    df = df.filter(df[value].isNotNull())

df = df.select("Year", "Month", "DayofMonth", "DayofWeek", "CRSDepTime", "CRSArrTime", "UniqueCarrier", "CRSElapsedTime", "Origin", "Dest", "Distance", "ArrDelay")

df = df.withColumn("ArrDelay", when(df["ArrDelay"] > 0, 1).otherwise(0))

df = df.withColumn("ArrDelay", df["ArrDelay"].cast("double"))

df = df.withColumn("CRSElapsedTime", df["CRSElapsedTime"].cast("int"))
df = df.withColumn("Distance", df["Distance"].cast("int"))

indexer = StringIndexer(inputCol="UniqueCarrier", outputCol="UniqueCarrierIndex")
df = indexer.fit(df).transform(df)
indexer = StringIndexer(inputCol="Origin", outputCol="OriginIndex")
df = indexer.fit(df).transform(df)
indexer = StringIndexer(inputCol="Dest", outputCol="DestIndex")
df = indexer.fit(df).transform(df)

df = df.withColumn("UniqueCarrierIndex", df["UniqueCarrierIndex"].cast("int"))
df = df.withColumn("OriginIndex", df["OriginIndex"].cast("int"))
df = df.withColumn("DestIndex", df["DestIndex"].cast("int"))

df = df.drop("UniqueCarrier", "Origin", "Dest")

df.printSchema()

df.show()

# 6 bonus) predikce zpoždění letů s logistickou regresí (metrika accuracy)

assembler = VectorAssembler(inputCols=["Year", "Month", "DayofMonth", "DayofWeek", "CRSDepTime", "CRSArrTime", "CRSElapsedTime", "Distance", "UniqueCarrierIndex", "OriginIndex", "DestIndex"], outputCol="features",
                            handleInvalid="skip")
df = assembler.transform(df)

train, test = df.randomSplit([0.9, 0.1], seed=42)

# lr = LogisticRegression(featuresCol="features", labelCol="ArrDelay")
# model = lr.fit(train)

rf = RandomForestClassifier(featuresCol="features", labelCol="ArrDelay")
model = rf.fit(train)

predictions = model.transform(test)

evaluator = MulticlassClassificationEvaluator(labelCol="ArrDelay", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print("Accuracy: ", accuracy)

spark.stop()