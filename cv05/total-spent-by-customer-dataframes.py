from pyspark.sql import SparkSession
from pyspark.sql.functions import round

spark = SparkSession.builder.master("spark://dce528d8129c:7077").appName("SparkSQL").getOrCreate()

# inferSchema slouzi k aut. zjisteni dat. typu
orders = spark.read.option("inferSchema", "true").csv("/files/cv05/customer-orders.csv")

# prejmenovani sloupcu, aby se s tim lepe pracovalo
orders = orders.withColumnRenamed("_c0", "customer_id").withColumnRenamed("_c1", "product_id").withColumnRenamed("_c2", "amount")

orders.printSchema()
# root
#  |-- customer_id: integer (nullable = true)
#  |-- product_id: integer (nullable = true)
#  |-- amount: double (nullable = true)

# 2) celkova vyse objednavek pro kazdeho zakaznika
# reseni pomoci DataFrames
count = orders.groupBy("customer_id").count().count()
orders.groupBy("customer_id").sum("amount").orderBy("customer_id").show(count, False)

# 2 bonus) zaokrouhleni castek na dve desetinna mista
# ulozeni do sloupce total_spent a serazeni
(orders.groupBy("customer_id")
    .sum("amount")
    .select("customer_id", round("sum(amount)", 2).alias("total_spent"))
    .orderBy("total_spent")
    .show(count, False))