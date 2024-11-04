"""
CV01 - web scraper
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import sys
from scraper_handler import get_articles_from_archive, process_article
from os import stat

sys.dont_write_bytecode = True

ARTICLE_LIST = []

# Number of thread pool threads
THREADS = 20

TASKS = []
PROCESSED_ARTICLES = []
FINISHED_ARTICLES_URLS = set()
FINISHED_ARTICLES_TITLES = set()

ARCHIVE_COUNTER = 900


def save_to_file(data: list, filename: str):
    """Save data to file."""

    # memory optimalisation with manual file writing
    with open(filename, "a+", encoding="utf8") as f:
        f.seek(0) # go to the beginning
        if f.read():
            f.seek(0, 2) # go to EOF
            f.seek(f.tell() - 2, 0) # go to last character (\n]) before EOF
            f.truncate() # remove last character (\n])
            f.write(",") # add comma for concat
            f.write(json.dumps(data, indent=2, ensure_ascii=False)[1:]) # write data without first character ([)
        else:
            json.dump(data, f, indent=2) # write data

def multithread_scrape():
    """Run multithreaded scraper."""
    global ARCHIVE_COUNTER
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        while ARTICLE_LIST or TASKS:
            while len(TASKS) <= THREADS:
                if len(ARTICLE_LIST) <= 10:
                    ARTICLE_LIST.extend(get_articles_from_archive(ARCHIVE_COUNTER))
                    ARCHIVE_COUNTER += 1
                url = ARTICLE_LIST.pop(0)
                if url not in FINISHED_ARTICLES_URLS:
                    FINISHED_ARTICLES_URLS.add(url)
                    print(f"{url} | PA: {len(PROCESSED_ARTICLES)} | FAU: {len(FINISHED_ARTICLES_URLS)}")
                    TASKS.append(executor.submit(process_article, url))
            
            for future in as_completed(TASKS):
                try:
                    [json_article, new_articles] = future.result()
                    PROCESSED_ARTICLES.append(json.loads(json_article))
                    ARTICLE_LIST.extend(new_articles)
                except Exception as e:
                    print(f"Error in task: {e}")
                finally:
                    TASKS.remove(future)
            print(f"Finished articles: {len(PROCESSED_ARTICLES)} | Finished urls: {len(FINISHED_ARTICLES_URLS)}")
            save_to_file(PROCESSED_ARTICLES, "cv01/articles.json")
            PROCESSED_ARTICLES.clear()

def load_previous_finished_files():
    """Load previously finished files."""
    with open("cv01/articles.json", "r", encoding="utf8") as f:
        data = json.load(f)
        for article in data:
            FINISHED_ARTICLES_URLS.add(article["url"])
            FINISHED_ARTICLES_TITLES.add(article["title"])
    with open("cv01/archive_counter.txt", "r", encoding="utf8") as f:
        global ARCHIVE_COUNTER
        ARCHIVE_COUNTER = int(f.read())

def singlethread_scrape():
    """Run singlethreaded scraper."""
    global ARCHIVE_COUNTER
    while True:
        while len(ARTICLE_LIST) > 10:
            url = ARTICLE_LIST.pop(0)
            if url not in FINISHED_ARTICLES_URLS:
                json_article = process_article(url)
                json_article = json.loads(json_article)
                if json_article["title"] not in FINISHED_ARTICLES_TITLES:
                    FINISHED_ARTICLES_URLS.add(url)
                    FINISHED_ARTICLES_TITLES.add(json_article["title"])
                    save_to_file([json_article], "cv01/articles.json")
        ARTICLE_LIST.extend(get_articles_from_archive(ARCHIVE_COUNTER))
        ARCHIVE_COUNTER += 1
        with open("cv01/archive_counter.txt", "w", encoding="utf8") as f:
            f.write(str(ARCHIVE_COUNTER))
            print(f"{ARCHIVE_COUNTER} | {stat("cv01/articles.json").st_size / (1024*1024)} MB")

def remove_redundancies():
    """Remove redundant articles from the file."""
    with open("cv01/articles.json", "r", encoding="utf8") as f:
        data = json.load(f)
        new_data = []
        for article in data:
            if article["title"] not in FINISHED_ARTICLES_TITLES and article["url"] not in FINISHED_ARTICLES_URLS:
                new_data.append(article)
                FINISHED_ARTICLES_TITLES.add(article["title"])
                FINISHED_ARTICLES_URLS.add(article["url"])
        with open("cv01/articles.json", "w", encoding="utf8") as f:
            json.dump(new_data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    # test_connection()
    remove_redundancies()
    load_previous_finished_files()
    singlethread_scrape()