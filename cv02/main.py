from datetime import datetime, timezone
import json

data = {}

# with open("cv01/articles.json", "r", encoding="utf-8") as f:
#     data = json.load(f)

# for article in data:
#     article["category"] = article["url"].split("www.idnes.cz/")[1].split("/")[0] if article["url"].startswith("https://www.idnes.cz/") else None
#     str = "\n".join(article["text"])
#     article["text"] = str

# for article in data:
#     if not article["time"]:
#         article["time"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
#     elif not (article["time"].endswith("+01:00") or article["time"].endswith("+02:00") or article["time"].endswith("+00:00")):
#         article["time"] += "+01:00"

# with open("cv02/articles.json", "w", encoding="utf-8") as f:
#     json.dump(data, f, indent=2, ensure_ascii=False)

# 1: vypište počet článků
print(f"1: {len(data)}")

# 2: vypište počet duplicitních článků
duplicate_count = 0
unique_article_urls = set()
unique_article_titles = set()
for article in data:
    if article["url"] in unique_article_urls or article["title"] in unique_article_titles:
        duplicate_count += 1
    else:
        unique_article_urls.add(article["url"])
        unique_article_titles.add(article["title"])
print(f"2: {duplicate_count}")

# 3: vypište datum nejstaršího článku
oldest_article = min(data, key=lambda x: datetime.strptime(x["time"], "%Y-%m-%dT%H:%M:%S%z"))
print(f"3: {oldest_article["title"]}: {oldest_article['time']}")

# 4: vypište jméno článku s nejvíce komentáři
most_commented_article = max(data, key=lambda x: x["comment_count"])
print(f"4: {most_commented_article['title']}: {most_commented_article['comment_count']}")

# 5: vypište nejvyšší počet přidaných fotek u článku
most_photos_article = max(data, key=lambda x: x["photo_count"])
print(f"5: {most_photos_article['photo_count']}: {most_photos_article['title']}")

# 6: vypište počty článků podle roku publikace
articles_by_year = {}
for article in data:
    year = article["time"].split("-")[0] if article["time"] else None 
    articles_by_year[year] = articles_by_year.get(year, 0) + 1
print("6: ", end="")
print([f"{k}: {v}" for k, v in articles_by_year.items()])

# 7: vypište počet unikátních kategorií a počet článků v každé kategorii
unique_categories = set()
articles_by_category = {}
for article in data:
    if article["category"] is not None:
        unique_categories.add(article["category"])
        articles_by_category[article["category"]] = articles_by_category.get(article["category"], 0) + 1
print("7: ", end="")
print([f"{k}: {v}" for k, v in articles_by_category.items()])

# 8: vypište 5 nejčastějších slov v názvu článků z roku 2021 (nemám v datasetu → použil jsem tedy 2020)
most_common_words = {}
for article in data:
    if article["time"] is not None and article["time"].startswith("2020"):
        for word in article["title"].split():
            most_common_words[word] = most_common_words.get(word, 0) + 1
most_common_words = dict(sorted(most_common_words.items(), key=lambda x: x[1], reverse=True))
print("8: ", end="")
print([f"{k}: {v}" for k, v in list(most_common_words.items())[:5]])

# 9: vypište celkový počet komentářů
total_comments = sum([article["comment_count"] for article in data])
print(f"9: {total_comments}")

# 10: vypište celkový počet slov ve všech článcích
total_words = sum([len(article["text"].split()) for article in data])
print(f"10: {total_words}")

# bonus 1: vypište 8 nejčastějších slov v článcích, odfiltrujte krátká slova (< 6 písmen)
most_common_words = {}
for article in data:
    for word in article["text"].lower().split():
        if len(word) >= 6:
            most_common_words[word] = most_common_words.get(word, 0) + 1
most_common_words = dict(sorted(most_common_words.items(), key=lambda x: x[1], reverse=True))
print("bonus 1: ", end="")
print([f"{k}: {v}" for k, v in list(most_common_words.items())[:8]])

# bonus 2: vypište 3 články s nejvyšším počtem výskytů slova Covid-19
articles_with_covid = {}
for article in data:
    articles_with_covid[article["title"]] = article["text"].count("Covid-19")
articles_with_covid = dict(sorted(articles_with_covid.items(), key=lambda x: x[1], reverse=True))
print("bonus 2: ", end="")
print([f"{k}: {v}" for k, v in list(articles_with_covid.items())[:3]])

# bonus 3: vypište články s nejvyšším a nejnižším počtem slov
most_words_article = max(data, key=lambda x: len(x["text"].split()))
least_words_article = min(data, key=lambda x: len(x["text"].split()))
print(least_words_article)
print(f"bonus 3: {most_words_article['title']}, {least_words_article['title']}")

# bonus 4: vypište průměrnou délku slova přes všechny články
total_word_length = sum([len(word) for article in data for word in article["text"].split()])
average_word_length = total_word_length / total_words
print(f"bonus 4: {average_word_length}")

# bonus 5: vypište měsíce s nejvíce a nejméně publikovanými články
articles_by_month = {}
for article in data:
    month = article["time"].split("-")[1] if article["time"] else None 
    articles_by_month[month] = articles_by_month.get(month, 0) + 1
most_articles_month = max(articles_by_month, key=articles_by_month.get)
least_articles_month = min(articles_by_month, key=articles_by_month.get)
print(f"bonus 5: {most_articles_month}, {least_articles_month}")