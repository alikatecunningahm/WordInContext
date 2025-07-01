import streamlit as st
from elasticsearch import Elasticsearch
from src.config import base as cfg
from elasticsearch.helpers import scan
from wordcloud import WordCloud, STOPWORDS
from pyvis.network import Network
from collections import defaultdict, Counter
import re
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter
import re
import networkx as nx

# --- Connect to Elasticsearch ---
es = Elasticsearch(cfg.ES_HOST)
es_verse_index = cfg.ES_VERSE_INDEX_NAME
es_strongs_id_index = cfg.ES_VERSE_INDEX_NAME

st.set_page_config(page_title="Bible Word Explorer", layout="wide")
st.title("📖 Bible Word Explorer")

# --- Sidebar ---
with st.sidebar:
    st.header("Search Filters")
    search_type = st.radio("Search for:", ["Strong's ID", "English word"])
    search_input = st.text_input(
        "Enter Strong's ID or English word:",
        value="G1411" if search_type == "Strong's ID" else ""
    )
    version_filter = st.selectbox(
        "Filter by version:",
        ["ASV", "KJV", "ESV", "NIV", "NLT", "LXX"]
    )
    search_triggered = st.button("Search")

# Clean search input
if search_type == "English word":
    search_input = search_input.lower().strip()
else:
    search_input = search_input.strip()

# --- Session State ---
if "base_query" not in st.session_state:
    st.session_state.base_query = None
if "book_selection" not in st.session_state:
    st.session_state.book_selection = None

# --- Build Query ---
if search_triggered:
    if not search_input.strip():
        st.warning("Please enter a search term.")
    else:
        must_clause = [{"term": {"hebrew_id": search_input}}] if search_type == "Strong's ID" else [{"match_phrase": {"verse_part": search_input}}]
        base_query = {"bool": {"must": must_clause, "filter": []}}
    if version_filter:
        base_query["bool"]["filter"].append({"term": {"version": version_filter}})
        st.session_state.base_query = base_query
        st.session_state.book_selection = None

# --- Perform Search ---
if st.session_state.base_query:
    base_query = st.session_state.base_query

    # --- Word Cloud ---
    st.subheader("☁️ Word Cloud of Translations")
    query_wc = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "unique_translations": {
                "terms": {"field": "verse_part.keyword", "size": 1000}
            }
        }
    }
    res_wc = es.search(index=cfg.ES_VERSE_INDEX_NAME, body=query_wc)
    buckets_wc = res_wc.get("aggregations", {}).get("unique_translations", {}).get("buckets", [])

    if buckets_wc:
        text_wc = " ".join([b["key"] for b in buckets_wc if b["key"]])
        wordcloud = WordCloud(
            width=800, height=400, background_color="white",
            stopwords=STOPWORDS, collocations=False
        ).generate(text_wc)
        fig, ax = plt.subplots()
        ax.imshow(wordcloud, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig)

    else:
        st.info("No translation results found.")

    # --- Frequency by Book ---
    st.subheader("📊 Frequency by Bible Book")
    agg_book = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "by_book": {
                "terms": {"field": "bible_book", "size": 100, "order": {"_key": "asc"}}
            }
        }
    }
    res_book = es.search(index=cfg.ES_VERSE_INDEX_NAME, body=agg_book)
    df_book = pd.DataFrame(res_book["aggregations"]["by_book"]["buckets"])
    if not df_book.empty:
        df_book.columns = ["Book", "Count"]
        st.bar_chart(df_book.set_index("Book"))
    else:
        st.info("No book frequency data available.")

    # --- Frequency by Testament ---
    st.subheader("🕊️ Frequency by Testament Type")
    agg_test = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "by_testament": {
                "terms": {"field": "testament_type", "size": 10}
            }
        }
    }
    res_test = es.search(index=cfg.ES_VERSE_INDEX_NAME, body=agg_test)
    df_test = pd.DataFrame(res_test["aggregations"]["by_testament"]["buckets"])
    if not df_test.empty:
        df_test.columns = ["Testament", "Count"]
        fig1, ax1 = plt.subplots()
        ax1.pie(df_test["Count"], labels=df_test["Testament"], autopct="%1.1f%%", startangle=90)
        ax1.axis("equal")
        st.pyplot(fig1)
    else:
        st.info("No testament data available.")

    # --- Frequency by Literary Type ---
    st.subheader("📖 Frequency by Literary Type")
    agg_lit = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "by_lit": {
                "terms": {"field": "lit_type", "size": 10}
            }
        }
    }
    res_lit = es.search(index=cfg.ES_VERSE_INDEX_NAME, body=agg_lit)
    df_lit = pd.DataFrame(res_lit["aggregations"]["by_lit"]["buckets"])
    if not df_lit.empty:
        df_lit.columns = ["Literary Type", "Count"]
        fig2, ax2 = plt.subplots()
        ax2.pie(df_lit["Count"], labels=df_lit["Literary Type"], autopct="%1.1f%%", startangle=90)
        ax2.axis("equal")
        st.pyplot(fig2)
    else:
        st.info("No literary type data available.")

    # --- Surrounding Words + Co-occurrence ---
    st.subheader("🔍 Surrounding Word Frequency & Co-occurrence Network")
    # --- Find Unique verses ---
    unique_verses_query = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "unique_verses": {
                "terms": {"field": "bible_verse", "size": 1000}
            }
        }
    }
    res_unique_verses = es.search(index=cfg.ES_VERSE_INDEX_NAME, body=unique_verses_query)
    buckets_unique_verses = res_unique_verses.get("aggregations", {}).get("unique_verses", {}).get("buckets", [])
    unique_verses = [bucket["key"] for bucket in buckets_unique_verses]

    # Use scan helper to handle scroll + batching
    all_verse_parts_results = scan(
        client=es,
        index=cfg.ES_VERSE_INDEX_NAME,
        query={
            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "bible_verse": unique_verses
                            }
                        }
                    ],
                    "filter": [
                        {
                            "term": {
                                "version": version_filter
                            }
                        }
                    ]
                }
            }
        },
        preserve_order=True
    )

    # Convert to list (we want all in memory at once)
    all_verse_parts = list(all_verse_parts_results)

    # 1️⃣ Group verse parts by bible_verse
    verse_texts = defaultdict(list)

    for doc in all_verse_parts:
        verse_id = doc['_source']['bible_verse']
        verse_part_text = doc['_source']['verse_part']
        verse_texts[verse_id].append(verse_part_text)

    # 2️⃣ Concatenate the parts for each verse
    concatenated_verses = {
        verse_id: " ".join(parts)
        for verse_id, parts in verse_texts.items()
    }

    # Build list of most common words
    word_counter = Counter()
    cooc_counter = Counter()

    custom_stopwords = STOPWORDS.union({"thee", "thou", "thy", "ye", "unto", "shall", "hath"})
    for verse_id, verse_text in concatenated_verses.items():
        words = [word for word in verse_text.split(" ") if word.lower().strip() not in custom_stopwords and word.lower().strip() != search_input]
        word_counter.update(words) 
    
    top_words = set([w for w, _ in word_counter.most_common(50)])

    for wlist in concatenated_verses.values():
        unique = set([w for w in wlist.split(" ") if w in top_words])
        for w1 in unique:
            for w2 in unique:
                if w1 < w2:
                    cooc_counter[(w1, w2)] += 1

    edges = [(w1, w2, c) for (w1, w2), c in cooc_counter.items() if c >= 1]

   # --- Build graph G_nx as before ---
    G_nx = nx.Graph()
    for w in top_words:
        G_nx.add_node(w, size=word_counter[w])
    for w1, w2, c in edges:
        G_nx.add_edge(w1, w2, weight=c)

    # Create PyVis network
    net = Network(notebook=False, height="700px", width="100%", bgcolor="#222222", font_color="white")

    # Load NetworkX graph into PyVis
    net.from_nx(G_nx)

    # Compute fixed layout with spring_layout and a fixed seed
    pos = nx.spring_layout(G_nx, seed=42, k=0.5, iterations=50)

    # Assign positions to nodes in PyVis (scale up positions for better display)
    for node in net.nodes:
        node_id = node["id"]
        if node_id in pos:
            node["x"] = pos[node_id][0] * 1000
            node["y"] = pos[node_id][1] * 1000

    # Disable physics so layout stays fixed and does not bounce
    net.toggle_physics(False)

    # Save and render in Streamlit
    net.save_graph("network.html")
    st.components.v1.html(open("network.html").read(), height=750, scrolling=False)

    # --- Verses using this search term ---
    # Section header
    st.markdown(f"## Verses using *{search_input}*")

    # Iterate over concatenated verses and display
    for verse_id, text in concatenated_verses.items():
        # Use regex to highlight all occurrences (case-insensitive)
        if search_type == "English word":
            search_word = search_input
        else:
            verse_query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "bible_verse": verse_id
                                }
                            }
                        ],
                        "filter": [
                            {
                                "term": {
                                    "version": version_filter
                                }
                            },
                            {
                                "term": {
                                    "hebrew_id": search_input
                                }
                            }
                        ]
                    }
                }
            }

            verse_query_results = es.search(index=cfg.ES_VERSE_INDEX_NAME, body=verse_query)
            search_word = verse_query_results['hits']['hits'][0]['_source']['verse_part']

        pattern = re.compile(re.escape(search_word), re.IGNORECASE)
        
        highlighted_text = pattern.sub(
            lambda m: f"<span style='background-color: #ccffcc; color: #006600'><b>{m.group(0)}</b></span>",
            text
        )
        
        # Display verse with highlighted term
        st.markdown(f"**{verse_id}**: {highlighted_text}", unsafe_allow_html=True)