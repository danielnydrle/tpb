from datetime import datetime
import json
import plotly.express as px

data = {}
with open("cv02/articles.json", "r", encoding="utf8") as f:
    data = json.load(f)

# 1 Vykreslete křivku zobrazující přidávání článků v čase
covid_by_time = {}
for article in data:
    time = article["time"][:10] if article["time"] else None
    covid_by_time[time] = covid_by_time.get(time, 0) + 1
covid_by_time = {k: v for k, v in covid_by_time.items() if k is not None}
covid_by_time = dict(sorted(covid_by_time.items(), key=lambda x: x[0]))
px.line(x=list(covid_by_time.keys()), y=list(covid_by_time.values()), title="Přidávání článků v čase").show()

# 2. Vykreslete sloupcový graf zobrazující počet článků v jednotlivých rocích
articles_by_year = {}
for article in data:
    year = article["time"].split("-")[0] if article["time"] else None   
    articles_by_year[year] = articles_by_year.get(year, 0) + 1
articles_by_year = {k: v for k, v in articles_by_year.items() if k is not None}
articles_by_year = dict(sorted(articles_by_year.items(), key=lambda x: x[0]))
px.bar(x=list(articles_by_year.keys()), y=list(articles_by_year.values()), title="Počet článků v jednotlivých rocích").show()

# 3. Vykreslete scatter graf zobrazující vztah mezi délkou článku a počtem komentářů
comments_by_length = {}
for article in data:
    l = len(article["text"])
    if l in comments_by_length:
        comments_by_length[l] += article["comment_count"]
    else:
        comments_by_length[l] = article["comment_count"]
px.scatter(x=comments_by_length.keys(), y=list(comments_by_length.values()), title="").show()

# 4. Vykreslete koláčový graf zobrazující podíl článků v jednotlivých kategoriích.
categories_by_articles = {}
for article in data:
    c = article["category"] 
    if c in categories_by_articles:
        categories_by_articles[c] += 1
    else:
        categories_by_articles[c] = 1
px.pie(names=list(categories_by_articles.keys()), values=list(categories_by_articles.values())).show()

# 5. Vykreslete histogram pro počet slov v článcích.
words_by_articles = {}
for article in data:
    words = len(article["text"].split())
    words_by_articles[words] = words_by_articles.get(words, 0) + 1
px.histogram(x=list(words_by_articles.keys()), y=list(words_by_articles.values()), title="Počet slov v článcích").show()

# 6. Vykreslete histogram pro délku slov v článcích.
word_length_by_articles = {}
for article in data:
    for word in article["text"].split():
        l = len(word)
        word_length_by_articles[l] = word_length_by_articles.get(l, 0) + 1
px.histogram(x=list(word_length_by_articles.keys()), y=list(word_length_by_articles.values()), title="Délka slov v článcích", nbins=100).show()

# 7. Vykreslete časovou osu zobrazující výskyt slova koronavirus v názvu článků. - Přidejte druhou křivku pro výraz vakcína.
covid_by_time = {}
vaccine_by_time = {}
for article in data:
    time = article["time"][:10] if article["time"] else None
    if "koronavirus" in article["title"]:
        covid_by_time[time] = covid_by_time.get(time, 0) + 1
    if "vakcína" in article["title"]:
        vaccine_by_time[time] = vaccine_by_time.get(time, 0) + 1
covid_by_time = {k: v for k, v in covid_by_time.items() if k is not None}
covid_by_time = dict(sorted(covid_by_time.items(), key=lambda x: x[0]))
vaccine_by_time = {k: v for k, v in vaccine_by_time.items() if k is not None}
vaccine_by_time = dict(sorted(vaccine_by_time.items(), key=lambda x: x[0]))
fig = px.line()
fig.add_scatter(x=list(covid_by_time.keys()), y=list(covid_by_time.values()), name="Koronavirus")
fig.add_scatter(x=list(vaccine_by_time.keys()), y=list(vaccine_by_time.values()), name="Vakcína")
fig.show()

# 8. Vykreslete histrogram pro počet článků v jednotlivých dnech týdne.
days = {}
for article in data:
    if article["time"]:
        day = datetime.strptime(article["time"], "%Y-%m-%dT%H:%M:%S%z").weekday()
        days[day] = days.get(day, 0) + 1
days = {k: v for k, v in days.items() if k is not None}
days = dict(sorted(days.items(), key=lambda x: x[0]))
px.histogram(x=list(days.keys()), y=list(days.values()), title="Počet článků v jednotlivých dnech týdne", nbins=7).show()