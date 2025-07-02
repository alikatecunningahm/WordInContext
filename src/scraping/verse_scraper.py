import os
import re
import time
import logging
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VerseScraper:
    def __init__(self, driver, search_terms, versions, home_page="https://www.eliyah.com/lexicon.html"):
        """
        Initialize the scraper with a Selenium driver, search terms, Bible versions, and homepage URL.
        """
        self.driver = driver
        self.search_terms = search_terms
        self.versions = versions
        self.home_page = home_page
 
    # Determine the book name from the first search term
    def _determine_bible_book(self):
        """Extract the Bible book name from the first search term."""
        first_term = self.search_terms[0]
        if len(re.findall(r"[A-Za-z]+", first_term)) > 1:
            self.bible_book = first_term
        elif re.match(r"^\d", first_term):
            self.bible_book = first_term.rsplit(" ", maxsplit=1)[0]
        else:
            self.bible_book = first_term.split(" ")[0]

    # Classify the book by literature type (e.g. Pentateuch, Wisdom)
    def _determine_lit_type(self):
        """Assign literature type based on Bible book."""
        book = self.bible_book
        if book in ["Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy"]:
            self.lit_type = "Pentateuch"
        elif book in ["Joshua", "Judges", "Ruth", "1 Samuel", "2 Samuel", "1 Kings", "2 Kings",
                      "1 Chronicles", "2 Chronicles", "Ezra", "Nehemiah", "Esther"]:
            self.lit_type = "Historical Books (Former Prophets)"
        elif book in ["Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Solomon"]:
            self.lit_type = "Wisdom Literature"
        elif book in ["Isaiah", "Jeremiah", "Lamentations", "Ezekial", "Daniel"]:
            self.lit_type = "Major Prophets"
        elif book in ["Hosea", "Joel", "Amos", "Obadiah", "Jonah", "Micah", "Nahum",
                      "Habakkuk", "Zephaniah", "Haggai", "Zechariah", "Malachi"]:
            self.lit_type = "Minor Prophets"
        elif book in ["Matthew", "Mark", "Luke", "John"]:
            self.lit_type = "Gospels"
        elif book == "Revelation":
            self.lit_type = "Apocalypse"
        else:
            self.lit_type = "Epistles"
        logger.info(f"📖 Literature type: {self.lit_type}")

    # Determine Old or New Testament
    def _determine_testament_type(self):
        """Assign Old or New Testament based on literature type."""
        if self.lit_type in ["Gospels", "Apocalypse", "Epistles"]:
            self.testament_type = "New Testament"
        else:
            self.testament_type = "Old Testament"

    # Create output directory for saving scraped data
    def _create_dir(self, version):
        """Create the output directory if it doesn't exist yet."""
        out_dir = Path(__file__).resolve().parents[1] / "scraped_docs" / "verse_data" / version
        if not os.path.exists(out_dir):
            logger.info(f"📁 Creating directory: {out_dir}")
            os.makedirs(out_dir)
        return out_dir

    # Load the lexicon homepage
    def _load_home_page(self):
        """Load the homepage of the lexicon search tool."""
        self.driver.get(self.home_page)

    # Execute search query on a term for a given version
    def _search_on_term(self, search_term, version):
        """Perform a search for a given term and Bible version."""
        self._load_home_page()

        search = self.driver.find_element_by_xpath('/html/body/main/div/div[1]/div/form/div/div/div[2]/div/input')
        search.clear()

        select = Select(self.driver.find_element_by_xpath('/html/body/main/div/div[1]/div/form/div/div/div[1]/select'))
        select.select_by_value(version)

        search.send_keys(search_term, Keys.RETURN)
        time.sleep(15)

    # Parse the table of verse parts (words/phrases) from a concordance view
    def _extract_verse_part_data(self, soup):

        """Extract individual word/phrase entries from a verse page."""
        table = soup.find("div", {"id": "concTable"})

        if not table:
            return []

        verse_parts = []

        for row in table.find_all("div", {"class": "row"}):
            dct = {}
            tcols = row.find_all("div")
            english_words = [a.text for a in tcols[0].find_all("a")]
            verse_part = " ".join(english_words)

            dct["verse_part_type"] = "PHRASE" if "PHRASE" in verse_part else "WORD"
            dct["verse_part"] = verse_part.replace("PHRASE", "").strip()

            try:
                hebrew_id = tcols[1].find("a")
                dct["hebrew_id"] = hebrew_id.text.upper()
            except Exception:
                dct["hebrew_id"] = None

            verse_parts.append(dct)

        return verse_parts

    # Build a DataFrame of structured results for a given search term
    def _build_dataframe_for_term(self, search_term):
        """Navigate through verse references and build DataFrame for one term."""
        self._search_on_term(search_term, version=self.current_version)

        soup = BeautifulSoup(self.driver.page_source, "lxml")
        trows = soup.find_all("div", id=re.compile("verse_*"))

        if not trows:
            return pd.DataFrame()

        all_parts = []

        for trow in trows:

            # For each row in table, find tag that contains verse number / detail link
            top_level_data = trow.find('a', attrs={'data-ev-label': lambda val: val and 'Verse Row [REF] BibleID' in val})

            # Determine verse number
            try:
                verse = top_level_data.text
            except NoSuchElementException:
                verse = "Omitted"

            details_link = self.driver.find_element("xpath", f"//a[@href='{top_level_data['href']}']")
            details_link.click()
            time.sleep(5)

            details_soup = BeautifulSoup(self.driver.page_source, "lxml")

            logger.info(f"🧩 Parsing verse: {verse}")

            # Extract parts
            parts = self._extract_verse_part_data(details_soup)
            for p in parts:
                if re.match(r"^\d", self.search_terms[0]):
                    p["bible_chapter"] = search_term.split(" ")[2]
                else:
                    p["bible_chapter"] = search_term.split(" ")[1]
                p["bible_verse"] = self.bible_book + verse
                all_parts.append(p)
            
            # Close details pop-up
            close_button = self.driver.find_element("xpath",'//*[@id="interClose"]')
            close_button.click()
            time.sleep(5)

        return pd.DataFrame(all_parts)

    # Scrape all search terms for a specific Bible version
    def _process_version(self, version):
        """Scrape all search terms and save results for one version."""
        self.current_version = version
        book_data = []

        for term in self.search_terms:
            if "Song" in term:
                term = term.replace("Song of Solomon", "Sng")

            logger.info(f"🔍 Scraping term: {term} | Version: {version}")

            while True:
                try:
                    df = self._build_dataframe_for_term(term)
                    if df.empty:
                        logger.warning(f"🛑 No data from {term}. Retrying...")
                        continue
                    book_data.append(df)
                    break
                except Exception as e:
                    logger.exception(f"❌ Error processing {term}: {e}")
                    time.sleep(300)

        book_df = pd.concat(book_data).reset_index(drop=True)
        book_df["lit_type"] = self.lit_type
        book_df["testament_type"] = self.testament_type
        book_df["bible_book"] = self.bible_book
        book_df["version"] = version

        out_dir = self._create_dir(version)
        out_path = out_dir / f"{self.bible_book}.csv"
        book_df.to_csv(out_path, index=False)
        logger.info(f"✅ Saved: {out_path}")

    # Main orchestrator function
    def run(self):
        """Run the full scraping process across all versions."""
        self._determine_bible_book()
        self._determine_lit_type()
        self._determine_testament_type()

        for version in self.versions:
            logger.info(f"🚀 Starting scrape for version: {version}")
            self._process_version(version)
