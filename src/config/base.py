import os

ES_INDEX_NAME = "bible_search_index"
ES_HOST = os.environ.get("ES_HOST", "http://localhost:9200")
ES_BASE_DIR = "../scraped_docs"
ES_TIMEOUT = 30