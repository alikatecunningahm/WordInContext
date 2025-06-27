from src.scraping.driver.connect import define_driver
from src.scraping.strongs_id_scraper import StrongsIDScraper
from src.scraping.verse_scraper import VerseScraper
from pathlib import Path
import pandas as pd
import os

driver = define_driver()

# Build lookups for Strong's ID terms 
# strongs_id_scraper = StrongsIDScraper(driver=driver)
# strongs_id_scraper.run()

# Define bible versions to iterate over
versions = pd.read_csv(Path.joinpath(Path(__file__).resolve().parents[1], 'versions', 'versions.csv'))
versions = versions['versions'].tolist()
versions = versions[2:]

search_term_dir = Path.joinpath(Path(__file__).resolve().parents[1], 'documents')
# Convert search_terms / versions to lists
for file in os.listdir(search_term_dir):
	if 'csv' in file:
		search_terms = pd.read_csv(os.path.join(search_term_dir,file))
		search_terms = search_terms['search_terms'].tolist()
		verse_scraper = VerseScraper(driver=driver,search_terms=search_terms,versions=versions)
		verse_scraper.run()