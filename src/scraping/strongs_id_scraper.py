from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from pathlib import Path
import pandas as pd
import logging
import time
import sys
import re
import os

# --- Logging Setup ---
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

class StrongsIDScraper:
    def __init__(self, driver, id_types=["Hebrew", "Greek"], home_page="https://biblehub.com/strongs.htm"):
        self.driver = driver
        self.id_types = id_types
        self.home_page = home_page

    def load_home_page(self):
        """Navigate to the BibleHub Strong's homepage."""
        self.driver.get(self.home_page)

    def fetch_strong_links(self):
        """Scrape and categorize Hebrew and Greek Strong's ID links."""
        soup = BeautifulSoup(self.driver.page_source, "lxml")
        links = soup.find_all("a", href=lambda href: href and "biblehub.com/strongs/" in href)

        self.hebrew_strong_links = [link for link in links if re.search(r"strongs/[a-i]", str(link))]
        self.greek_strong_links = [link for link in links if re.search(r"strongs/[j-o]", str(link))]

    def build_strong_df(self, id_type, link_list):
        """Scrape individual Strong's ID pages and return a DataFrame."""
        all_data = []

        for link in link_list:
            full_link = "https://" + link["href"]
            self.driver.get(full_link)
            soup = BeautifulSoup(self.driver.page_source, "lxml")

            id_links = soup.find_all("a", href=lambda href: href and f"/{id_type.lower()}/" in str(href))
            for id_link in id_links:
                full_id_link = "https://biblehub.com/" + id_link["href"]
                logger.info(f"📖 Scraping: {full_id_link}")

                while True:
                    try:
                        self.driver.refresh()
                        self.driver.get(full_id_link)
                        soup = BeautifulSoup(self.driver.page_source, "lxml")

                        id_data = {}
                        match = re.search(rf"(?<={id_type.lower()}/)\d+", full_id_link)
                        if not match:
                            continue  # Skip if no ID found

                        id_data["strongs_id"] = id_type.upper()[0] + match.group()

                        for tag in soup.find_all("span", class_="tophdg"):
                            label = tag.get_text(strip=True).rstrip(":")
                            if label == "Original Word":
                                lang_tag = tag.find_next_sibling("span", class_=id_type.lower())
                                value = lang_tag.get_text(strip=True) if lang_tag else ""
                            else:
                                next_node = tag.next_sibling
                                while next_node and (getattr(next_node, "name", None) or str(next_node).strip() == ""):
                                    next_node = next_node.next_sibling
                                value = str(next_node).strip() if next_node else ""

                            id_data[label] = value

                        # Success - exit loop
                        break

                    except Exception as e:
                        logging.warning(f"Error processing {full_id_link} - {e}. Retrying in 30s...")
                        time.sleep(30)

                all_data.append(id_data)

        return pd.DataFrame(all_data)

    @staticmethod
    def create_output_dir(id_type):
        """Create output directory if it doesn't exist."""
        out_dir = Path(__file__).resolve().parents[1] / "scraped_docs" / "id_lookups" / id_type
        if not out_dir.exists():
            logger.info(f"📁 Creating output directory: {out_dir}")
            os.makedirs(out_dir)
        return out_dir

    def store_id_data(self):
        """Scrape data for all selected ID types and save them to CSV."""
        for id_type in self.id_types:
            attr_name = f"{id_type.lower()}_strong_links"
            link_list = getattr(self, attr_name, [])
            df = self.build_strong_df(id_type=id_type, link_list=link_list)

            out_dir = self.create_output_dir(id_type)
            out_path = out_dir / f"{id_type.lower()}_id_lookup.csv"
            df.to_csv(out_path, index=False)
            logger.info(f"✅ Saved {len(df)} records to {out_path}")

    def run(self):
        """Orchestrate the full scraping process."""
        self.load_home_page()
        self.fetch_strong_links()
        self.store_id_data()
