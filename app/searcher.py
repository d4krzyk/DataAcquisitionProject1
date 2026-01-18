import os
import math
import json
import logging
import apache_beam as beam
from indexer import tokenize, compute_tf
from database import fetch_all_documents

_qvec_cache = {}

# Ukrycie ostrzeżenia z wewnętrznych transformów Beam (np. _TopPerBundle)
logging.getLogger('apache_beam.transforms.core').setLevel(logging.ERROR)

def _col(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m"

def compute_idf(token, df_dict, N):
    smooth = os.getenv('SMOOTH_IDF', '1') == '1'
    df_val = df_dict.get(token, 0)
    if smooth:
        return math.log((1 + N) / (1 + df_val)) + 1.0
    return math.log(N / df_val) if df_val > 0 else 0.0

def cosine_similarity(vec_a, vec_b):
    dot = sum(v * vec_b.get(k, 0.0) for k, v in vec_a.items())
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)

def _read_docs_worker(_):
    # Worker czyta DB -> buduje TF z content
    rows = fetch_all_documents()
    for row in rows:
        db_id, path, content, created_at = row
        text = content or ""
        tf = compute_tf(tokenize(text))
        yield {"db_id": db_id, "path": str(path), "tf": tf}

def _tf_to_tfidf(tf_dict: dict, df_dict: dict, n_docs: int) -> dict:
    return {t: tf * compute_idf(t, df_dict, n_docs) for t, tf in tf_dict.items()}

def _build_query_vec(query: str, df_dict: dict, n_docs: int) -> dict:
    q_tf = compute_tf(tokenize(query))
    return {t: tf * compute_idf(t, df_dict, n_docs) for t, tf in q_tf.items()}

def run_search_pipeline(query, top_n=10, similarity_fn=None):
    sim = similarity_fn or cosine_similarity

    # Informacje widoczne w terminalu głównym
    print(_col(f"Rozpoczynam wyszukiwanie dla: {query}", "1;36"), flush=True)
    print(_col("Uruchamiam pipeline Beam...", "1;33"), flush=True)
    print(_col("Czytam dokumenty z bazy...", "0;37"), flush=True)

    with beam.Pipeline() as p:
        # inicjalny element, żeby wywołać czytanie w workerze
        docs_pc = (
            p
            | 'Init' >> beam.Create([None])
            | 'ReadFromDB' >> beam.FlatMap(_read_docs_worker)
        )

        total_docs = docs_pc | 'CountDocs' >> beam.combiners.Count.Globally()

        token_doc_ones = docs_pc | 'TokensPerDoc' >> beam.FlatMap(
            lambda d: ((t, 1) for t in d.get('tf', {}).keys()))
        df_pc = token_doc_ones | 'CountDF' >> beam.CombinePerKey(sum)

        scored = docs_pc | "ScoreDocs" >> beam.Map(
            lambda d, df_dict, n_docs: (
                d["path"],
                sim(
                    _build_query_vec(query, df_dict, n_docs),
                    _tf_to_tfidf(d.get("tf", {}), df_dict, n_docs),
                ),
            ),
            beam.pvalue.AsDict(df_pc),
            beam.pvalue.AsSingleton(total_docs),
        )

        top = scored | 'TopN' >> beam.combiners.Top.Of(top_n, key=lambda x: x[1])

        _ = (
            top
            | 'FlattenTop' >> beam.FlatMap(lambda lst: lst)
            | 'PrintResults' >> beam.Map(
                lambda r: print(_col(f"{r[0]}  score={r[1]:.6f}", "1;32"), flush=True)
            )
        )

    print(_col("Zakończono pipeline. Wyniki wypisane powyżej.", "1;36"), flush=True)
    return None



if __name__ == "__main__":
    try:
        while True:
            q = input("Wpisz zapytanie (ENTER aby zakończyć): ").strip()
            top_n = input("Ile wyników pokazać (domyślnie 10): ").strip()
            if top_n.isdigit():
                top_n = int(top_n)
            else:
                top_n = 10
            if not q:
                break
            run_search_pipeline(query=q, top_n=top_n)
        print("\nKończę działanie wyszukiwarki.\n")
        exit(0)
    except (EOFError, KeyboardInterrupt):
        pass
