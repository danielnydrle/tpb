from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("spark://a382251566a2:7077").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/files/customer-orders.csv")
def parseLine(line):
    fields = line.split(',')
    customerID = fields[0]
    productID = fields[1]
    amount = float(fields[2])
    return (customerID, productID, amount)

parsedLines = lines.map(parseLine)
customerAmounts = parsedLines.map(lambda x: (x[0], x[2]))
totalAmounts = customerAmounts.reduceByKey(lambda x, y: x + y)
results = totalAmounts.collect()

for result in results:
    print(f"{result[0]}: {result[1]:.2f}")

print("#" * 50)

# bonus - setrideni s pomoci RDD

sortedResults = totalAmounts.sortBy(lambda x: x[1], ascending=False).collect()
for result in sortedResults:
    print(f"{result[0]}: {result[1]:.2f}")