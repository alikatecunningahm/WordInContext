# 🕮 WordInContext

**WordInContext** is an interactive Bible exploration tool that lets users search for words and phrases across scripture and view them in full context. It also scrapes and connects Strong’s Concordance definitions to deepen word-level study.

---

## 🧠 How It Works

Selenium with geckodriver scrapes Strong’s definitions from BibleHub
Elasticsearch stores structured Bible text for fast querying
Streamlit powers the user interface for search, context viewing, and visualizations

## 🚀 Features

- 🔍 Search for words or phrases across the entire Bible
- 📖 See the context: Book, chapter, verse, and full text
- 📊 Visualize distribution of words and usage patterns
- 📚 Strong’s Concordance integration via automated scraping
- 💡 Designed for deeper Bible study, word analysis, and visualization

---

## 📦 Prerequisites

- Python 3.10+
- Poetry or pip for dependency management
- **Elasticsearch** (recommended version: 8.x)  
  Must be running at `http://localhost:9200` or configured in `src/config/base.py`
- **Geckodriver** (for Firefox + Selenium scraping)  
  - [Download from Mozilla](https://github.com/mozilla/geckodriver/releases)  
  - Or install via Homebrew (macOS):  
    ```bash
    brew install geckodriver
    ```
- (Optional) Docker and Docker Compose

---

## ⚙️ Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/alikatecunningahm/WordInContext.git
   cd WordInContext```

   ## ⚙️ Installation

2. **Install Python Requirements

   ```pip install -r requirements.txt```

3. **Ensure Elasticsearch is running** 
You can start a local instance with Docker:
```docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:8.13.0```

4. **Run the scraping + indexing pipeline**
   ```python examples/run_app.py```

5. **Launch the Streamlit app**
   ```streamlit run src/bible_explorer_app.py```
   Then open your browser to http://localhost:8501.
