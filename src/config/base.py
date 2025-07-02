import os

STRONGS_DATA_FOLDER = "id_lookups"
VERSE_DATA_FOLDER = "verse_data"
ES_VERSE_INDEX_NAME = "verse_index"
ES_STRONGS_INDEX_NAME = "strongs_id_index"
ES_HOST = os.environ.get("ES_HOST", "http://localhost:9200")
ES_BASE_DIR = "../scraped_docs"
ES_TIMEOUT = 30