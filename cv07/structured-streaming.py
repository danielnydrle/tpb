from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, current_timestamp, window

spark = SparkSession.builder.master("spark://a76fb3e6550e:7077").appName("StructuredStreaming").getOrCreate()

# 1) pocitani cetnosti slov v textu vstupniho streamu
# data pres netcat na portu 9999
# predzpracovani slov - mala pismena, odstraneni interpunkce
# agregace podle okenka, razeni podle zacatku okenka a cetnosti
# vypis na konzoli

lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

lines_with_timestamp = lines.withColumn("timestamp", current_timestamp())

words = lines_with_timestamp \
    .select(
        explode(
            split(lower(col("value")), "\\s+")
        ).alias("word"),
        col("timestamp")
    ) \
    .filter(col("word").rlike("^[a-záčďéěíňóřšťúůýž]+$"))

# okenko 30 sekund s posunem 15 sekund
windowedCounts = words \
    .groupBy(
        window(col("timestamp"), "30 seconds", "15 seconds"),
        col("word")
    ) \
    .count() \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("word"),
        col("count")
    ) \
    .orderBy(col("window_start"), col("count").desc())

query = windowedCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()