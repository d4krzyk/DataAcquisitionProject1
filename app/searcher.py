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
     else:
         return math.log(N / df_val) if df_val > 0 else 0.0

def cosine_similarity(vec_a, vec_b):
    dot = sum(v * vec_b.get(k, 0.0) for k, v in vec_a.items())
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _build_qvec_local(query, df_dict, N):
    tokens = tokenize(query)
    tf = compute_tf(tokens)
    qvec = {}
    for token, tf_val in tf.items():
        idf = compute_idf(token, df_dict, N)
        qvec[token] = tf_val * idf
    return qvec

def score_doc_worker(doc, df_dict, N, query, sim=None):
    key = query
    if key not in _qvec_cache:
        _qvec_cache[key] = _build_qvec_local(query, df_dict, N)
    qvec = _qvec_cache[key]
    sfn = sim or cosine_similarity
    return doc['db_id'], doc['filename'], sfn(qvec, doc.get('tfidf', {}))

def _read_docs_worker(_):
    # Wywoływane na workerze; używa fetch_all_documents() z app.database
    rows = fetch_all_documents()
    for row in rows:
        db_id, filename, index_structure = row
        if isinstance(index_structure, str):
            try:
                index_structure = json.loads(index_structure)
            except Exception:
                index_structure = {}
        tfidf = index_structure.get('tfidf', {}) if isinstance(index_structure, dict) else {}
        yield {'db_id': db_id, 'filename': filename, 'tfidf': tfidf}


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
            | 'LogRead' >> beam.Map(lambda d: (print(_col(f"[READ] id={d.get('db_id')} file={d.get('filename')}", "0;34"), flush=True), d)[1])
        )

        total_docs = docs_pc | 'CountDocs' >> beam.combiners.Count.Globally()

        # pokazywanie liczby dokumentów (drukowane w workerze, zwykle pojedynczy wpis)
        _ = total_docs | 'LogTotal' >> beam.Map(lambda n: (print(_col(f"[COUNT] Łącznie dokumentów: {n}", "1;32"), flush=True), n)[1])

        token_doc_ones = docs_pc | 'TokensPerDoc' >> beam.FlatMap(lambda d: ((t, 1) for t in d.get('tfidf', {}).keys()))
        df_pc = token_doc_ones | 'CountDF' >> beam.CombinePerKey(sum)

        # ocena dokumentów się rozpoczyna
        docs_for_scoring = docs_pc | 'LogScoringStart' >> beam.Map(lambda d: (print(_col(f"[SCORE] Przygotowuję do oceny: {d.get('filename')}", "0;33"), flush=True), d)[1])

        scored = docs_for_scoring | 'ScoreDocs' >> beam.Map(
            score_doc_worker,
            beam.pvalue.AsDict(df_pc),
            beam.pvalue.AsSingleton(total_docs),
            query,
            sim
        )

        top = scored | 'TopN' >> beam.combiners.Top.Of(top_n, key=lambda x: x[2])

        _ = (
            top
            | 'FlattenTop' >> beam.FlatMap(lambda lst: lst)
            | 'PrintResults' >> beam.Map(lambda t: (print(_col(f"[RESULT] {t[0]} {t[1]} score={t[2]:.6f}", "1;32"), flush=True), t)[1])
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
