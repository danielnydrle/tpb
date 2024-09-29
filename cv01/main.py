"""
CV01 - web scraper
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import sys
from pymongo.errors import ConnectionFailure
from db_handler import MONGO_CLIENT
from scraper_handler import get_article_urls, get_page, get_articles_from_homepage, process_article

sys.dont_write_bytecode = True

ARTICLE_LIST = []

# Number of thread pool threads
THREADS = 5

TASKS = []
PROCESSED_ARTICLES = []

# Number of articles needed for saving to file
SAVE_THRESHOLD = 10

def test_connection():
    """Test connection to MongoDB."""
    try:
        MONGO_CLIENT.admin.command("ismaster")
    except ConnectionFailure:
        print("MongoDB connection error.")
        sys.exit(1)


def save_to_file(data: list, filename: str):
    """Save data to file."""
    # old_data = []
    # with open(filename, "r+", encoding="utf-8") as f:
    #     old_data = json.load(f) if f.read() else []
    # old_data.extend(data)
    # with open(filename, "w+", encoding="utf-8") as f:
    #     f.seek(0)
    #     json.dump(old_data, f, indent=2)
    #     f.truncate()
    #     PROCESSED_ARTICLES.clear()

    # memory optimalisation with manual file writing
    with open(filename, "a+", encoding="utf-8") as f:
        f.seek(0) # go to the beginning
        if f.read():
            f.seek(0, 2) # go to EOF
            f.seek(f.tell() - 2, 0) # go to last character (\n]) before EOF
            f.truncate() # remove last character (\n])
            f.write(",") # add comma for concat
            f.write(json.dumps(data, indent=2, ensure_ascii=False)[1:]) # write data without first character ([)
        else:
            json.dump(data, f, indent=2) # write data

        PROCESSED_ARTICLES.clear()

def multithread_scrape():
    """Run multithreaded scraper."""
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        while ARTICLE_LIST or TASKS:
            if len(ARTICLE_LIST) >= THREADS and len(TASKS) < THREADS:
                for url in ARTICLE_LIST[:THREADS]:
                    TASKS.append(executor.submit(process_article, url))
                    ARTICLE_LIST.remove(url)
            
            for future in as_completed(TASKS):
                try:
                    [json_article, new_articles] = future.result()
                    PROCESSED_ARTICLES.append(json.loads(json_article, ))
                    ARTICLE_LIST.extend(new_articles)
                except Exception as e:
                    print(f"Error in task: {e}")
                TASKS.remove(future)
            if (len(PROCESSED_ARTICLES) > 0) and (len(PROCESSED_ARTICLES) % SAVE_THRESHOLD < THREADS):
                save_to_file(PROCESSED_ARTICLES, "cv01/articles.json")

if __name__ == "__main__":
    test_connection()
    init_articles = get_articles_from_homepage()
    ARTICLE_LIST.extend(init_articles)
    multithread_scrape()