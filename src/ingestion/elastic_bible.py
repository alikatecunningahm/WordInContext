import os
import time
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from config import base as cfg

# # ---- CONFIG ----
# INDEX_NAME = "bible_search_index"
# cfg.ES_HOST = "http://localhost:9200"
# CSV_FOLDER = "../scraped_docs"

# ---- STEP 1: CONNECT TO ELASTICSEARCH ----
from elasticsearch import Elasticsearch

def wait_for_es():
    print("🔌 Waiting for Elasticsearch to be available...")
    es = Elasticsearch(
        cfg.ES_HOST,
        verify_certs=False,
        ssl_show_warn=False
    )
    for i in range(30):
        try:
            if es.ping():
                print("✅ Elasticsearch is up!")
                return es
        except Exception as e:
            print(f"⏳ Still waiting... ({e})")
        time.sleep(2)
    raise RuntimeError(f"❌ Could not connect to Elasticsearch at {cfg.ES_HOST}")


# ---- STEP 2: DEFINE INDEX MAPPING ----
def create_index(es):
    if es.indices.exists(index=cfg.ES_INDEX_NAME):
        print(f"⚠️ Deleting existing index '{cfg.ES_INDEX_NAME}'")
        es.indices.delete(index=cfg.ES_INDEX_NAME)

    mapping = {
        "mappings": {
            "properties": {
                "bible_book": {"type": "keyword"},
                "bible_chapter": {"type": "integer"},
                "bible_verse": {"type": "keyword"},
                "verse_part_type": {"type": "keyword"},
                "verse_part": {"type": "text",
                	"fields": {
                    			"keyword": {"type": "keyword"}
                			}
            	},
                "hebrew_id": {"type": "keyword"},
                "lit_type": {"type": "keyword"},
                "testament_type": {"type": "keyword"},
                "version": {"type": "keyword"},
            }
        }
    }

    es.indices.create(index=cfg.ES_INDEX_NAME, body=mapping)
    print(f"✅ Created index '{cfg.ES_INDEX_NAME}' with mapping.")

# ---- STEP 3: INGEST CSV FILES ----
def ingest_csv(es, filepath):
    print(f"📥 Ingesting: {filepath}")
    df = pd.read_csv(filepath)
    df["bible_chapter"] = pd.to_numeric(df["bible_chapter"], errors="coerce")
    df = df.fillna("")

    actions = [
        {
            "_index": cfg.ES_INDEX_NAME,
            "_source": row.to_dict()
        }
        for _, row in df.iterrows()
    ]
    bulk(es, actions)
    print(f"✅ Ingested {len(actions)} records from {filepath}")
    
def ingest_all_csvs(es, folder):
    
	# List of version directories
	version_dir = [version for version in os.listdir(folder) if "DS_Store" not in version]
    
	for version in version_dir:
         version_path = os.path.join(folder,version)
        
         filenames = [f for f in os.listdir(version_path) if f.endswith(".csv")]
         for file in filenames:
             filepath = os.path.join(version_path,file)
             ingest_csv(es,filepath)
             
# ---- MAIN SCRIPT ----
if __name__ == "__main__":
    es = wait_for_es()
    create_index(es)
    ingest_all_csvs(es, CSV_FOLDER)