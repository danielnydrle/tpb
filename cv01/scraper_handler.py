"""
Scraper handler module.
"""

import json
from itertools import chain
from urllib.parse import unquote
import requests
import requests.cookies
from pyquery import PyQuery

s = requests.session()
s.cookies.set(name="euconsent-v2", value="CQFwyQAQFwyQAAHABBENBIFgAP_gAAAAAAAAJqIBJC5kBSFCAGJgYNkAIAAWxxAAIAAAABAAgAAAABoAIAgAEAAwAAQABAAAABAAIEIAAABACABAAAAAQAAAAQAAAAAQAAAAAQAAAAAAAiBACAAAAABAAQAAAABAQAAAgAAAAAIAQAAAAAEAgAAAAAAAAAAAABAAAQgAAAAAAAAAAAAAAAAAAAAAAAAAABBAAAAAAAAAAAAAAAAAwAAADBIAMAAQU6HQAYAAgp0SgAwABBTopABgACCnRCADAAEFOi0AGAAIKdAA.f_wAAAAAAAAA")
s.cookies.set(name="dCMP", value="mafra=1111,all=1,reklama=1,part=0,cpex=1,google=1,gemius=1,id5=1,next=0000,onlajny=0000,jenzeny=0000,databazeknih=0000,autojournal=0000,skodahome=0000,skodaklasik=0000,groupm=1,piano=1,seznam=1,geozo=0,czaid=1,click=1,verze=2,")

def get_page(url: str):
    """Get html page source."""
    res = s.get(url, cookies=s.cookies)
    return res.text

def get_article_urls(html: str):
    """Get article urls from page."""
    pq = PyQuery(html)
    article_urls = pq("[score-type='Article']").items()
    chain(article_urls, pq("a.art-link").items(), pq("a.aside").items())
    return article_urls

def get_article_data(url: str, html: str):
    """Parse article data."""
    pq = PyQuery(html)
    
    title = pq.find("h1").text()
    text = [pq.find(".opener").text()] + [PyQuery(e).text() for e in pq.find("#art-text p")]
    tags = [PyQuery(e).text() for e in pq.find("#art-tags > a")]
    photo_count = int(pq.find(".more-gallery b").text()[0]) if pq.find(".more-gallery b").text() else 0
    time = pq.find(".art-info .time span.time-date").attr("content")
    comment_count = int(pq.find("#moot-linkin span").text().split()[0][1:]) if pq.find("#moot-linkin span").text() else 0

    data = {
        "url": url,
        "title": title,
        "text": text,
        "tags": tags,
        "photo_count": photo_count,
        "time": time,
        "comment_count": comment_count
    }

    if data["text"] is None:
        return
    else:
        return data

def get_articles_from_homepage():
    """Get homepage article urls."""
    hp = get_page("https://idnes.cz")
    urls = get_article_urls(hp)
    return [unquote(url.attr("href")) for url in urls]

def get_articles_from_archive(index: int):
    """Get archive article urls."""
    archive = get_page(f"https://www.idnes.cz/zpravy/archiv/{index}")
    urls = get_article_urls(archive)
    return [unquote(url.attr("href")) for url in urls]

def process_article(url):
    """
    Process article.
    1) Get article html
    2) Parse article data
    3) Save article data to json file
    4) Return list of new article urls
    """
    try:
        html = get_page(unquote(url))
        article_data = get_article_data(url, html)
        json_article_data = json.dumps(article_data, indent=2, ensure_ascii=False)

        articles = get_article_urls(html)
        hrefs = [(article.attr("href") if "/_servix/" not in article.attr("href") else article.attr("href").split("url=")[1].split("%3fzdroj")[0]) for article in articles]
        return [json_article_data, hrefs]
    except Exception as e:
        print(f"Error processing article {url}: {e}")
        return []