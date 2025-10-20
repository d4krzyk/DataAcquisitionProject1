# language: python
# File: app/indexer.py
import os
import math
import uuid

import apache_beam as beam
from database import create_table, insert_document

STOPWORDS = {'i', 'oraz', 'a', 'ale', 'to', 'na', 'w', 'z', 'do', 'się', 'jest'}

def read_file(path):
    with open(path, encoding='utf-8') as f:
        return f.read()

def tokenize(text):
    return [t for t in text.lower().split() if t not in STOPWORDS]

def compute_tf(tokens):
    tf = {}
    for token in tokens:
        tf[token] = tf.get(token, 0) + 1
    total = len(tokens)
    return {k: v / total for k, v in tf.items()}

def run_pipeline(docs_dir='docs'):
    file_paths = [os.path.join(docs_dir, f) for f in os.listdir(docs_dir)
                  if os.path.isfile(os.path.join(docs_dir, f))]
    N = len(file_paths)
    create_table()  # spróbuj stworzyć tabelę (z retry w database.py)

    if N == 0:
        return

    with beam.Pipeline() as p:
        paths = p | 'CreatePaths' >> beam.Create(file_paths)

        doc_tokens = (
            paths
            | 'ReadFiles' >> beam.Map(lambda path: (path, tokenize(read_file(path))))
        )

        doc_tf = doc_tokens | 'ComputeTF' >> beam.Map(lambda kv: (kv[0], compute_tf(kv[1])))

        token_doc_ones = doc_tokens | 'UniqueTokensPerDoc' >> beam.FlatMap(
            lambda kv: ((token, 1) for token in set(kv[1]))
        )

        df = token_doc_ones | 'CountDF' >> beam.CombinePerKey(sum)

        def compute_tfidf_element(elem, df_dict, total_docs):
            path, tf_dict = elem
            tfidf = {}
            for token, tf_value in tf_dict.items():
                df_val = df_dict.get(token, 1)
                tfidf[token] = tf_value * math.log(total_docs / df_val)
            return (path, tfidf)

        doc_tfidf = doc_tf | 'ComputeTFIDF' >> beam.Map(
            compute_tfidf_element,
            beam.pvalue.AsDict(df),
            N
        )
        # Generujemy UUID, zapisujemy pełną ścieżkę oraz reprezentację w jednym dict
        (
                doc_tfidf
                | 'ToDB' >> beam.Map(
            lambda kv: insert_document(
                str(uuid.uuid4()),
                {'path': os.path.abspath(kv[0]), 'tfidf': kv[1]}
            )
        )



        )

if __name__ == '__main__':
    run_pipeline()