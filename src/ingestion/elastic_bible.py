# Import required libraries
import os
import time
import pandas as pd
import json
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from src.config import base as cfg  # Custom config file with paths and ES settings

# Define the directory containing Elasticsearch index mappings (JSON format)
CONFIG_DIR = os.path.join(os.path.dirname(__file__), '..', 'config', 'es_mappings')
# Define the directory containing scraped verse & strong id data
BASE_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'scraped_docs')

# Load variables from .env file
load_dotenv()

def wait_for_es():
    """
    Waits for the Elasticsearch service to be available.
    Tries to ping the Elasticsearch host up to 30 times with 2-second intervals.
    Raises a RuntimeError if ES is not reachable after all attempts.
    """
    print("🔌 Waiting for Elasticsearch to be available...")

    es = Elasticsearch(
    hosts=os.getenv('ES_HOST'),
    api_key=os.getenv('ES_API_KEY'),
    verify_certs=True
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


def load_mapping(mapping_file):
    """
    Loads an Elasticsearch index mapping from a JSON file.

    :param mapping_file: Name of the JSON file containing the mapping.
    :return: Parsed JSON object as a Python dictionary.
    """
    with open(os.path.join(CONFIG_DIR, mapping_file), 'r') as f:
        return json.load(f)


def create_index(es, index_name, mapping):
    """
    Creates a new index in Elasticsearch with the given mapping.
    If the index already exists, it will be deleted first.

    :param es: Elasticsearch client instance.
    :param index_name: Name of the index to create.
    :param mapping: Mapping schema as a dictionary.
    """
    if es.indices.exists(index=index_name):
        print(f"⚠️ Deleting existing index '{index_name}'")
        es.indices.delete(index=index_name)

    es.indices.create(index=index_name, body=mapping)
    print(f"✅ Created index '{index_name}' with mapping.")


def ingest_csv(es, index_name, filepath):
    """
    Ingests data from a single CSV file into the specified Elasticsearch index.

    :param es: Elasticsearch client instance.
    :param index_name: Target index for data ingestion.
    :param filepath: Full path to the CSV file.
    """
    print(f"📅 Ingesting: {filepath} into '{index_name}'")
    df = pd.read_csv(filepath)

    # Ensure bible_chapter column is numeric if it exists
    if 'bible_chapter' in df.columns:
        df["bible_chapter"] = pd.to_numeric(df["bible_chapter"], errors="coerce")

    # Replace NaN values with empty strings for clean ingestion
    df = df.fillna("")

    # Create a list of ES bulk actions from dataframe rows
    actions = [{"_index": index_name, "_source": row.to_dict()} for _, row in df.iterrows()]

    # Bulk ingest to Elasticsearch
    bulk(es, actions)
    print(f"✅ Ingested {len(actions)} records from {filepath}")


def ingest_csvs_in_folder(es, index_name, folder, nested=False):
    """
    Ingests multiple CSV files from a directory (optionally from nested folders).

    :param es: Elasticsearch client instance.
    :param index_name: Target index for ingestion.
    :param folder: Path to the folder containing CSV files.
    :param nested: Whether the folder contains nested subdirectories.
    """
    if nested:
        # Loop through version-named subdirectories
        version_dirs = [v for v in os.listdir(folder) if not v.startswith('.')]
        for version in version_dirs:
            version_path = os.path.join(folder, version)
            filenames = [f for f in os.listdir(version_path) if f.endswith(".csv")]
            for file in filenames:
                filepath = os.path.join(version_path, file)
                ingest_csv(es, index_name, filepath)
    else:
        # Ingest directly from flat directory
        filenames = [f for f in os.listdir(folder) if "DS_Store" not in f]
        for file in filenames:
            filepath = os.path.join(folder, file)
            ingest_csv(es, index_name, filepath)


# ---- MAIN EXECUTION BLOCK ----
if __name__ == "__main__":
    # Establish connection to Elasticsearch
    es = wait_for_es()
    base_data_folder = os.path.join(os.path.dirname(__file__), '..', '..')

    # ---- STEP 1: Ingest Bible Verse Parts ----
    verse_mapping = load_mapping("verse_mapping.json")  # Load verse mapping definition
    create_index(es, cfg.ES_VERSE_INDEX_NAME, verse_mapping)  # Create verse index
    ingest_csvs_in_folder(es, cfg.ES_VERSE_INDEX_NAME, os.path.join(BASE_DATA_DIR,cfg.VERSE_DATA_FOLDER), nested=True)  # Ingest nested CSVs

    # ---- STEP 2: Ingest Strongs ID Data ----
    strongs_mapping = load_mapping("strongs_id_mapping.json")  # Load strongs mapping definition
    create_index(es, cfg.ES_STRONGS_INDEX_NAME, strongs_mapping)  # Create strongs index
    ingest_csvs_in_folder(es, cfg.ES_STRONGS_INDEX_NAME, os.path.join(BASE_DATA_DIR,cfg.STRONGS_DATA_FOLDER,'Hebrew'), nested=False)  # Ingest Hebrew ID csvs
    ingest_csvs_in_folder(es, cfg.ES_STRONGS_INDEX_NAME, os.path.join(BASE_DATA_DIR,cfg.STRONGS_DATA_FOLDER,'Greek'), nested=False)  # Ingest Greek ID csvs
