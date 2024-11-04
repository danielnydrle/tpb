from datetime import datetime
import json
import sys
from pymongo import MongoClient

CLIENT = MongoClient("mongodb://localhost:27017")

try:
    CLIENT.server_info()
except Exception as e:
    print(f"MongoDB is not running: {e}")
    sys.exit(e)

DB = CLIENT["articles"]
COLLECTION = DB["articles"]

data = {}
with open("cv02/articles.json", "r", encoding="utf8") as f:
    data = json.load(f)

# COLLECTION.insert_many(data)

# 1. Vypište náhodný článek
random_article = COLLECTION.aggregate([{"$sample": {"size": 1}}]).next()
print(f"1. {random_article}\n")

# 2. Vypište celkový počet článků
article_count = COLLECTION.count_documents({})
print(f"2. {article_count}\n")

# 3. Vypište průměrný počet fotek na článek
average_photos_per_article = COLLECTION.aggregate([
    {
        "$group": {
            "_id": None,
            "avg": {
                "$avg": "$photo_count"
            }
        }
    }
]).next()["avg"]
print(f"3. {average_photos_per_article}")
CNT = 0
for art in data:
    CNT += art["photo_count"]
print(f"3.1 {CNT / len(data)}\n")

# 4. Vypište počet článků s více než 100 komentáři
articles_with_more_than_100_comments = COLLECTION.count_documents({"comment_count": {"$gt": 100}})
print(f"4. {articles_with_more_than_100_comments}")

# 4.1 kontrola v Pythonu
CNT = 0
for art in data:
    if art["comment_count"] > 100:
        CNT += 1
print(f"4.1 {CNT}\n")

# 5. Pro každou kategorii vypište počet článků z roku 2022 (nemám v datasetu → použil jsem tedy 2020)
articles_by_category = COLLECTION.aggregate([
    {
        "$match": {
            "time": {
                "$ne": None,
                "$regex": "^2020"
            }
        }
    },
    {
        "$group": {
            "_id": "$category",
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {"count": -1}
    }
])

print("5.")
for article in articles_by_category:
    print(f"{article["_id"]}: {article["count"]}")