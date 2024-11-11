import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType

spark = SparkSession.builder \
    .master("spark://a382251566a2:7077") \
    .appName("WordCount") \
    .getOrCreate()

sc = spark.sparkContext

def pseudo_nltk(word: str) -> str:
    if not word or word.startswith('http'):
        return ''
    word = word.lower()
    word = word.replace('.', '').replace(',', '').replace('!', '').replace('?', '').replace('(', '').replace(')', '')
    return word

input = sc.textFile("/files/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()
sortedCounts = sorted(
    [(pseudo_nltk(word.encode("ascii", "ignore").decode()), count)
     for word, count in wordCounts.items()
     if pseudo_nltk(word)],
    key=lambda x: x[1],
    reverse=True
)
for word, count in sortedCounts[:20]:
    print(f"{word}: {count}")

print("#" * 50)

# Bonus - idnes data

df = spark.read.option("multiline", "true").json("/files/articles.json")
df.printSchema()

words = df.rdd.flatMap(lambda x: x['text'].split())
wordCounts = words.countByValue()
sortedCounts = sorted(
    [(pseudo_nltk(word), count)
     for word, count in wordCounts.items()
     if pseudo_nltk(word) and len(pseudo_nltk(word)) >= 6],
    key=lambda x: x[1],
    reverse=True
)

for word, count in sortedCounts[:20]:
    print(f"{word}: {count}")