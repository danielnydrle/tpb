"""
Scraper handler module.
"""
from datetime import datetime
import json
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from pyquery import PyQuery

# Headless chrome options
CHROME_OPTIONS = webdriver.ChromeOptions()
CHROME_OPTIONS.add_argument('--headless')
CHROME_OPTIONS.add_argument('--no-sandbox')
CHROME_OPTIONS.add_argument('--disable-dev-shm-usage')
CHROME_OPTIONS.binary_location = "/usr/bin/chromium-browser"

# Path to chromedriver
CHROME_SERVICE = Service('/usr/bin/chromedriver')

# Virtual chrome instance
DRIVER = webdriver.Chrome(options=CHROME_OPTIONS, service=CHROME_SERVICE)

def get_page(url: str, wait_s: int = 5, is_with_cookie_consent: bool = False):
    """Get html page source."""
    DRIVER.get(url)
    if is_with_cookie_consent:
        wait = WebDriverWait(DRIVER, wait_s)
        wait.until(lambda d: d.find_element(By.CLASS_NAME, "contentwall_ok"))
        button = DRIVER.find_element(By.CLASS_NAME, "contentwall_ok")
        button.click()
        wait.until(lambda d: d.find_element(By.CSS_SELECTOR, "[score-type='Article']"))
    return DRIVER.page_source

def get_article_urls(html: str):
    """Get article urls from page."""
    pq = PyQuery(html)
    article_urls = pq("[score-type='Article']").items()
    return article_urls

def get_article_data(url, html):
    """Parse article data."""
    pq = PyQuery(html)
    return {
        "url": url,
        "title": pq.find("h1").text(),
        "text": [pq.find(".opener").text()] + [PyQuery(e).text() for e in pq.find("#art-text p")],
        "tags": [PyQuery(e).text() for e in pq.find("#art-tags > a")],
        "photo_count": int(pq.find(".more-gallery span").text()[0]) if pq.find(".more-gallery span").text() else 0,
        "time": pq.find(".art-info .time span.time-date").attr("content"),
        "comment_count": int(pq.find("#moot-linkin span").text().split()[0][1:]) if pq.find("#moot-linkin span").text() else 0,
    }

def get_articles_from_homepage():
    """Get homepage article urls."""
    hp = get_page("https://idnes.cz", wait_s=5, is_with_cookie_consent=True)
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
        html = get_page(url, wait_s=5)
        article_data = get_article_data(url, html)
        json_article_data = json.dumps(article_data, indent=2, ensure_ascii=False)

        articles = get_article_urls(html)
        return [json_article_data, [article.attr("href") for article in articles]]
    except Exception as e:
        print(f"Error processing article {url}: {e}")
        return []