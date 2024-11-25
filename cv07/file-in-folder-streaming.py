from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, regexp_replace

spark = SparkSession.builder.master("spark://a76fb3e6550e:7077").appName("FileInFolderStreaming").getOrCreate()

# 2 bonus) cetnost slov v souborech pridavanych do adresare
# vstupem bude kazdy soubor vlozeny do vybraneho adresare
# vyhodnoceni by melo byt spusteno po kazdem vlozenem souboru
# vypis na konzoli

lines = spark.readStream \
    .format("text") \
    .option("path", "/files/cv07/streaming") \
    .option("wholetext", "false") \
    .load()

# Zpracování slov
words = lines \
    .select(
        explode(
            split(
                regexp_replace(
                    lower(col("value")), 
                    "[^a-záčďéěíňóřšťúůýž0-9\\s]", 
                    " "
                ),
                "\\s+"
            )
        ).alias("word")
    ) \
    .filter(col("word").rlike("^[a-záčďéěíňóřšťúůýž]+$"))

wordCounts = words \
    .groupBy("word") \
    .count() \
    .orderBy(col("word")) \
    .orderBy(col("count").desc())

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()