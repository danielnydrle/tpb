"""
Scraper handler module.
"""

import json
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
    return article_urls

def get_article_data(url, html):
    """Parse article data."""
    pq = PyQuery(html)
    
    data = {
        "url": url,
        "title": pq.find("h1").text(),
        "text": [pq.find(".opener").text()] + [PyQuery(e).text() for e in pq.find("#art-text p")],
        "tags": [PyQuery(e).text() for e in pq.find("#art-tags > a")],
        "photo_count": int(pq.find(".more-gallery span").text()[0]) if pq.find(".more-gallery span").text() else 0,
        "time": pq.find(".art-info .time span.time-date").attr("content"),
        "comment_count": int(pq.find("#moot-linkin span").text().split()[0][1:]) if pq.find("#moot-linkin span").text() else 0,
    }

    if data["text"] is None:
        return
    else:
        return data

def get_articles_from_homepage():
    """Get homepage article urls."""
    hp = get_page("https://idnes.cz")
    urls = get_article_urls(hp)
    return [url.attr("href") for url in urls]

def process_article(url):
    """
    Process article.
    1) Get article html
    2) Parse article data
    3) Save article data to json file
    4) Return list of new article urls
    """
    try:
        html = get_page(url)
        article_data = get_article_data(url, html)
        json_article_data = json.dumps(article_data, indent=2, ensure_ascii=False)

        articles = get_article_urls(html)
        return [json_article_data, [article.attr("href") for article in articles]]
    except Exception as e:
        print(f"Error processing article {url}: {e}")
        return []