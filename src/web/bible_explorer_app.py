import streamlit as st
from elasticsearch import Elasticsearch
from src.config import base as cfg
from elasticsearch.helpers import scan
from wordcloud import WordCloud, STOPWORDS
from pyvis.network import Network
from collections import defaultdict, Counter
import re
import pandas as pd
from collections import Counter
import re
import networkx as nx
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

# --- Read in cfg vars ---
es_verse_index = cfg.ES_VERSE_INDEX_NAME
es_strongs_id_index = cfg.ES_STRONGS_INDEX_NAME

# --- Connect to Elasticsearch ---
es = Elasticsearch(
    hosts=st.secrets["ES_HOST"],
    api_key=st.secrets["ES_API_KEY"],
    verify_certs=True
    )

st.set_page_config(page_title="Bible Word Explorer", layout="wide")
st.title("📖 Bible Word Explorer")

# Fetch Strong's IDs for dropdown
def get_strongs_options():
    query = {
        "size": 10000,
        "_source": ["strongs_id", "Original Word", "Transliteration"],
        "query": {"match_all": {}}
    }
    res = es.search(index=es_strongs_id_index, body=query)
    options = res.get("hits", {}).get("hits", [])
    
    # Map: "Transliteration – Original Word (Strong's ID)" -> strongs_id
    return {
        f"{doc['_source'].get('Transliteration', '')} – {doc['_source'].get('Original Word', '')} ({doc['_source']['strongs_id']})":
        doc['_source']['strongs_id']
        for doc in options if 'strongs_id' in doc['_source']
    }

# --- Sidebar ---
with st.sidebar:
    st.header("Search Filters")
    search_type = st.radio("Search for:", ["Strong's ID", "English word"])
    
    search_input = ""
    if search_type == "Strong's ID":
        strongs_options = get_strongs_options()
        selected_option = st.selectbox("Choose Strong's ID:", list(strongs_options.keys()))
        search_input = strongs_options[selected_option]
    else:
        search_input = st.text_input("Enter English word:", "")

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

if "filter_path" not in st.session_state:
    st.session_state.filter_path = []

if st.session_state.get("filter_path"):
    display_term = " → ".join(st.session_state["filter_path"])
else:
    display_term = search_input  # fallback to initial input

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
    # Helper to rebuild base_query from a term
    def build_base_query(term):
        if search_type == "Strong's ID":
            return {
                "bool": {
                    "must": [{"term": {"hebrew_id": term}}],
                    "filter": [{"term": {"version": version_filter}}]
                }
            }
        else:
            return {
                "bool": {
                    "must": [{"match_phrase": {"verse_part": term}}],
                    "filter": [{"term": {"version": version_filter}}]
                }
            }

    # Display current filter path
    if st.session_state.get("filter_path"):
        path_str = " → ".join(st.session_state["filter_path"])
        st.markdown(f"### Current Filter Path: *{path_str}*")

        # Show controls only if more than one filter has been applied
        if len(st.session_state["filter_path"]) > 1:
            col1, col2 = st.columns([1, 1])

            with col1:
                if st.button("🔙 Step Back One Level"):
                    st.session_state.filter_path.pop()
                    last_term = st.session_state.filter_path[-1]
                    st.session_state.base_query = build_base_query(last_term)
                    st.session_state.search_input_filtered = last_term
                    st.rerun()

            with col2:
                if st.button("🔄 Reset Filters"):
                    original_term = st.session_state.filter_path[0]
                    st.session_state.filter_path = [original_term]
                    st.session_state.base_query = build_base_query(original_term)
                    st.session_state.search_input_filtered = original_term
                    st.rerun()

    # 💡 REFRESH base_query in local scope
    base_query = st.session_state.base_query

    # --- Summary Stats Panel ---
    st.subheader(f"📌 Summary Statistics: {display_term}")

    # Total number of occurrences
    total_occurrences_query = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "total_occurrences": {
                "value_count": {"field": "verse_part.keyword"}
            }
        }
    }
    res_total = es.search(index=es_verse_index, body=total_occurrences_query)
    total_occurrences = res_total.get("aggregations", {}).get("total_occurrences", {}).get("value", 0)

    # Distinct number of books
    distinct_books_query = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "distinct_books": {
                "cardinality": {"field": "bible_book"}
            }
        }
    }
    res_books = es.search(index=es_verse_index, body=distinct_books_query)
    distinct_books = res_books.get("aggregations", {}).get("distinct_books", {}).get("value", 0)

    # Unique verses
    unique_verses_query = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "unique_verses": {
                "cardinality": {"field": "bible_verse"}
            }
        }
    }
    res_unique_verses = es.search(index=es_verse_index, body=unique_verses_query)
    unique_verse_count = res_unique_verses.get("aggregations", {}).get("unique_verses", {}).get("value", 0)

    # Display in two columns
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(label="📚 Total Occurrences", value=f"{int(total_occurrences):,}")

    with col2:
        st.metric(label="📖 Books Containing Term", value=f"{int(distinct_books)}")

    with col3:
        st.metric(label="🔢 Unique Verses", value=f"{int(unique_verse_count)}")

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
    res_book = es.search(index=es_verse_index, body=agg_book)
    df_book = pd.DataFrame(res_book["aggregations"]["by_book"]["buckets"])
    if not df_book.empty:
        df_book.columns = ["Book", "Count"]
        df_book_sorted = df_book.sort_values("Count", ascending=False)
        fig = px.bar(
            df_book_sorted,
            x="Book",
            y="Count",
            title="Frequency by Bible Book",
            labels={"Count": "Occurrences"},
        )
        fig.update_layout(xaxis={'categoryorder':'total descending'})  # Ensures x-axis is sorted
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No book frequency data available.")

    # --- Frequency by Testament ---
    agg_test = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "by_testament": {
                "terms": {"field": "testament_type", "size": 10}
            }
        }
    }
    res_test = es.search(index=es_verse_index, body=agg_test)
    df_test = pd.DataFrame(res_test["aggregations"]["by_testament"]["buckets"])
    if not df_test.empty:
        df_test.columns = ["Testament", "Count"]

    # --- Frequency by Literary Type ---
    agg_lit = {
        "size": 0,
        "query": base_query,
        "aggs": {
            "by_lit": {
                "terms": {"field": "lit_type", "size": 10}
            }
        }
    }
    res_lit = es.search(index=es_verse_index, body=agg_lit)
    df_lit = pd.DataFrame(res_lit["aggregations"]["by_lit"]["buckets"])
    if not df_lit.empty:
        df_lit.columns = ["Literary Type", "Count"]

    # --- Display side-by-side pie charts ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🕊️ Frequency by Testament Type")
        if not df_test.empty:
            fig1 = px.pie(
                df_test,
                names="Testament",
                values="Count",
                title="",
                hole=0,  # Set >0 for donut chart
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No testament data available.")

    with col2:
        st.subheader("✍️ Frequency by Literary Type")
        if not df_lit.empty:
            fig2 = px.pie(
                df_lit,
                names="Literary Type",
                values="Count",
                title="",
                hole=0,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No literary type data available.")

    # --- Word Cloud ---
    st.subheader("☁️ Word Cloud of Translations")
    # Word cloud terms with counts
    term_counts = []
    text_wc = ""

    if search_type == "Strong's ID":
        # Word cloud based on English words related to the Strong's ID
        query_wc = {
            "size": 0,
            "query": base_query,
            "aggs": {
                "unique_translations": {
                    "terms": {"field": "verse_part.keyword", "size": 1000}
                }
            }
        }
        res_wc = es.search(index=es_verse_index, body=query_wc)
        buckets_wc = res_wc.get("aggregations", {}).get("unique_translations", {}).get("buckets", [])
        term_counts = [(b["key"], b["doc_count"]) for b in buckets_wc]
        text_wc = " ".join([b["key"] for b in buckets_wc if b["key"]])

    else:
        # Word cloud based on Strong's IDs associated with the English word
        strongs_id_agg_query = {
            "size": 0,
            "query": base_query,
            "aggs": {
                "unique_strongs_ids": {
                    "terms": {"field": "hebrew_id", "size": 1000}
                }
            }
        }
        res_strongs = es.search(index=es_verse_index, body=strongs_id_agg_query)
        buckets_strongs = res_strongs.get("aggregations", {}).get("unique_strongs_ids", {}).get("buckets", [])
        term_counts = [(b["key"], b["doc_count"]) for b in buckets_strongs]

    # Sort by count descending
    term_counts = sorted(term_counts, key=lambda x: -x[1])

    # Prepare dict for word cloud
    wc_freqs = {term: count for term, count in term_counts}

    col_wc, col_filter = st.columns([2, 1])

    with col_wc:
        if wc_freqs:
            wordcloud = WordCloud(
                width=800, height=400, background_color="white",
                stopwords=STOPWORDS, collocations=False
            ).generate_from_frequencies(wc_freqs)
            
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.imshow(wordcloud, interpolation="bilinear")
            ax.axis("off")
            st.pyplot(fig)
        else:
            st.info("No word cloud data found.")


    with col_filter:
        st.subheader("🔎 Filter Word Cloud Terms")

        filter_labels = [f"{term} ({count})" for term, count in term_counts]
        term_lookup = {f"{term} ({count})": term for term, count in term_counts}

        selected = st.selectbox(
            "Choose a word cloud term to filter by:",
            filter_labels,
            index=None,
            placeholder="Select a term...",
        )

        if selected:
            term = term_lookup[selected]

            if search_type == "English word":
                st.session_state.base_query["bool"]["must"].append({"term": {"hebrew_id": term}})
            else:
                st.session_state.base_query["bool"]["must"].append({"term": {"verse_part.keyword": term}})

            if not st.session_state.filter_path:
                st.session_state.filter_path.append(search_input)
            st.session_state.filter_path.append(term)
            st.session_state.search_input_filtered = term
            st.rerun()

    # --- Surrounding Words + Co-occurrence ---
    st.subheader("🔍 Surrounding Word Co-occurrence Heatmap")
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
    res_unique_verses = es.search(index=es_verse_index, body=unique_verses_query)
    buckets_unique_verses = res_unique_verses.get("aggregations", {}).get("unique_verses", {}).get("buckets", [])
    unique_verses = [bucket["key"] for bucket in buckets_unique_verses]

    # Use scan helper to handle scroll + batching
    all_verse_parts_results = scan(
        client=es,
        index=es_verse_index,
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

    custom_stopwords = STOPWORDS.union({"thee", "thou", "thy", "ye", "unto", "shall", "hath", ""})
    for verse_id, verse_text in concatenated_verses.items():
        words = [word for word in verse_text.split(" ") if word.lower().strip() not in custom_stopwords and word.lower().strip() != search_input]
        word_counter.update(words) 
    
    top_words = set([w for w, _ in word_counter.most_common(30)])

    for wlist in concatenated_verses.values():
        unique = set([w for w in wlist.split(" ") if w in top_words])
        for w1 in unique:
            for w2 in unique:
                if w1 < w2:
                    cooc_counter[(w1, w2)] += 1

    edges = [(w1, w2, c) for (w1, w2), c in cooc_counter.items() if c >= 1]

    # Convert top_words to a sorted list
    top_words_list = sorted(top_words)
    word_index = {word: i for i, word in enumerate(top_words_list)}

    # Initialize co-occurrence matrix
    cooc_matrix = np.zeros((len(top_words_list), len(top_words_list)))

    # Fill matrix using cooc_counter
    for (w1, w2), count in cooc_counter.items():
        if w1 in word_index and w2 in word_index:
            i, j = word_index[w1], word_index[w2]
            cooc_matrix[i, j] = count
            cooc_matrix[j, i] = count  # Symmetric

    # Create DataFrame for seaborn
    df_cooc = pd.DataFrame(cooc_matrix, index=top_words_list, columns=top_words_list)

    fig_heat = px.imshow(
    df_cooc,
    labels=dict(x="Word", y="Word", color="Co-occurrence"),
    x=top_words_list,
    y=top_words_list,
    color_continuous_scale="YlGnBu",
    aspect="auto"
)

    fig_heat.update_layout(
        title="",
        height=600,
        margin=dict(l=50, r=50, t=50, b=50)
    )

    st.plotly_chart(fig_heat, use_container_width=True)
    
    # --- Verses using this search term ---
    # Section header
    st.markdown(f"## Verses using *{display_term}*")

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

            verse_query_results = es.search(index=es_verse_index, body=verse_query)
            search_word = verse_query_results['hits']['hits'][0]['_source']['verse_part']

        pattern = re.compile(re.escape(search_word), re.IGNORECASE)
        
        highlighted_text = pattern.sub(
            lambda m: f"<span style='background-color: #ccffcc; color: #006600'><b>{m.group(0)}</b></span>",
            text
        )
        
        # Display verse with highlighted term
        st.markdown(f"{verse_id}: {highlighted_text}", unsafe_allow_html=True)