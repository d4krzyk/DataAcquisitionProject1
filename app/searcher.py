import os
import re
import math
import json
import threading
import queue
import traceback
import unicodedata
import sys
import apache_beam as beam
from indexer import tokenize, compute_tf
from database import fetch_all_documents


def build_corpus_from_db():
    rows = fetch_all_documents()  # (id, filename, index_structure)
    docs = []
    for row in rows:
        db_id, filename, index_structure = row
        if isinstance(index_structure, str):
            try:
                index_structure = json.loads(index_structure)
            except Exception:
                index_structure = {}
        tfidf = index_structure.get('tfidf', {}) if isinstance(index_structure, dict) else {}
        docs.append({'db_id': db_id, 'filename': filename, 'tfidf': tfidf})
    return docs

def compute_df(docs):
    df = {}
    for d in docs:
        for token in d['tfidf'].keys():
            df[token] = df.get(token, 0) + 1
    return df

def compute_idf(token, df_dict, N):
    smooth = os.getenv('SMOOTH_IDF', '1') == '1'
    df_val = df_dict.get(token, 0)
    if smooth:
        return math.log((1 + N) / (1 + df_val)) + 1.0
    else:
        return math.log(N / df_val) if df_val > 0 else 0.0

def compute_query_tfidf(query, df_dict, N):
    tokens = tokenize(query)
    tf = compute_tf(tokens)
    q_tfidf = {}
    for token, tf_val in tf.items():
        idf = compute_idf(token, df_dict, N)
        q_tfidf[token] = tf_val * idf
    return q_tfidf

def cosine_similarity(vec_a, vec_b):
    dot = sum(v * vec_b.get(k, 0.0) for k, v in vec_a.items())
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)

def search(query, similarity_fn=None):
    docs = build_corpus_from_db()
    N = len(docs)
    if N == 0:
        print("Brak dokumentów w indeksie.")
        return []
    df = compute_df(docs)
    qvec = compute_query_tfidf(query, df, N)
    sim = similarity_fn or cosine_similarity
    results = []
    for d in docs:
        score = sim(qvec, d['tfidf'])
        results.append((d['db_id'], d['filename'], score))
    results.sort(key=lambda x: x[2], reverse=True)
    topk = results[:N]

    for db_id, filename, score in topk:
        print(f"{db_id} {filename} score={score:.6f}")

    return topk


if __name__ == "__main__":
    # top_n = int(os.getenv("SEARCH_TOP", "10"))
    try:
        while True:
            q = input("Wpisz zapytanie (ENTER aby zakończyć): ").strip()
            if not q:
                break
            search(q)
        print("\nKończę działanie wyszukiwarki.\n")
        exit(0)
    except (EOFError, KeyboardInterrupt):
        pass

