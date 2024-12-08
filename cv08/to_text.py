import json

article_counter = 0

while article_counter < 50000:
    with open("cv08/articles.json", "r", encoding="utf-8") as f:
        # load the whole file as json
        articles = json.load(f)
        for article in articles:
            article_counter += 1
            with open("cv08/articles_normalized.txt", "a", encoding="utf-8") as f:
                f.write(article["text"] + "\n")
                f.close()