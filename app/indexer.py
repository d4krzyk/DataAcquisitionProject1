# language: python
# File: app/indexer.py
import os
import math
import uuid
import re
import unicodedata
import apache_beam as beam
try:
    from .database import create_table, insert_document
except Exception:
    from database import create_table, insert_document

STOPWORDS = {
    # polskie podstawowe
    'i', 'oraz', 'a', 'aż', 'za', 'aby' ,'ale', 'to', 'na', 'w', 'z', 'ze', 'do', 'się', 'jest',
    'być', 'by', 'że', 'czy', 'jak', 'o', 'u', 'po', 'od', 'dla', 'bez', 'przez',
    'pod', 'nad', 'między', 'me', 'mi', 'mnie', 'mną', 'mój', 'moja', 'moje', 'twój',
    'twoja', 'twoje', 'nasz', 'nasza', 'nasze', 'wasz', 'wasza', 'wasze',
    'jego', 'jej', 'ich', 'nam', 'wam', 'ej', 'im', 'ci', 'te', 'ta', 'ten', 'tego', 'tej',
    'tym', 'tam', 'tu', 'tuż', 'tutaj', 'tamto', 'tenże', 'tę', 'też', 'również',
    'jednak', 'więc', 'następnie', 'ponieważ', 'gdy', 'kiedy', 'dopóki', 'jeśli',
    'jeżeli', 'chociaż', 'aczkolwiek', 'tak', 'nie', 'nigdy', 'zawsze', 'czasami',
    'przed', 'poza', 'ile', 'który', 'która', 'które', 'którzy', 'których',
    'któremu', 'którym', 'no', 'mu', 'ma', 'ach', 'hej',
    '0','1','2','3','4','5','6','7','8','9',
    'the','and','is','in','of','a','for','on','with','that','this','it',
    'raz','dwa','trzy','cztery','pięć','ile','którykolwiek','sam','sama','same',
    'sobie','sobą','przy','razem','czyli','albo','ani','zatem','bowiem',
    'tymczasem','natomiast','acz','głównie','mniej','bardziej','no','także',
    'czyż','ot','już','dopiero','ponadto','dalej',
    'bo', 'co', 'ty', 'ją', 'cię', 'lub', 'oto', 'chce', 'twe', 'twa', 'go'
}

def read_file(path):
    with open(path, encoding='utf-8') as f:
        return f.read()
    return None


def tokenize(text):
    if not text:
        return []
    # normalizacja (usuwa dziwne formy Unicode)
    text = unicodedata.normalize('NFKC', text)
    # usuń interpunkcję, podziel na tokeny, filtruj stopwords i zbyt krótkie tokeny
    text = re.sub(r'[^\w\sąćęłńóśżźĄĆĘŁŃÓŚŻŹ]', ' ', text, flags=re.UNICODE)
    tokens = [t for t in text.lower().split()
              if len(t) > 1 and not t.isdigit() and t not in STOPWORDS]
    return tokens

def compute_tf(tokens):
    total = len(tokens)
    if total == 0:
        return {}
    counts = {}
    for t in tokens:
        counts[t] = counts.get(t, 0) + 1
    return {t: c / total for t, c in counts.items()}

# def compute_counts(tokens):
#     """Zwraca dict token -> liczba wystąpień w dokumencie."""
#     counts = {}
#     for t in tokens:
#         counts[t] = counts.get(t, 0) + 1
#     return counts

def compute_tfidf_element(elem, df_dict, total_docs):
    """
    Oblicza TF-IDF; domyślnie używa math.log(N/df) (zgodnie z testami).
    Jeśli ustawisz SMOOTH_IDF = 1 w środowisku, użyje idf = log((1+N)/(1+df)) + 1
    by uniknąć wartości zero.
    """
    path, tf_dict = elem
    tfidf = {}
    smooth = os.getenv('SMOOTH_IDF', '1') == '1'
    for token, tf_value in tf_dict.items():
        df_val = df_dict.get(token, 1)
        if smooth:
            idf = math.log((1 + total_docs) / (1 + df_val)) + 1.0
        else:
            idf = math.log(total_docs / df_val) if df_val > 0 else 0.0
        tfidf[token] = tf_value * idf
    return (path, tfidf)

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
        # doc_counts = doc_tokens | 'Counts' >> beam.Map(lambda kv: (kv[0], compute_counts(kv[1])))

        doc_tf = doc_tokens | 'ComputeTF' >> beam.Map(lambda kv: (kv[0], compute_tf(kv[1])))

        token_doc_ones = doc_tokens | 'UniqueTokensPerDoc' >> beam.FlatMap(lambda kv:
                                                    ((token, 1) for token in set(kv[1])))

        df = token_doc_ones | 'CountDF' >> beam.CombinePerKey(sum)

        doc_tfidf = doc_tf | 'ComputeTFIDF' >> beam.Map(compute_tfidf_element,
                                                        beam.pvalue.AsDict(df),N)
        # Generujemy UUID, zapisujemy pełną ścieżkę oraz reprezentację w jednym dict
        (
                doc_tfidf | 'ToDB' >> beam.Map(lambda kv: insert_document(
                str(uuid.uuid4()),{'path': os.path.abspath(kv[0]), 'tfidf': kv[1],'counts': counts_map.get(kv[0],{})}
        )
        # (
        #     doc_tfidf
        #     | 'ToDB' >> beam.Map(
        #         lambda kv, counts_map: insert_document(
        #             str(uuid.uuid4()),
        #             {'path': os.path.abspath(kv[0]), 'tfidf': kv[1], 'counts': counts_map.get(kv[0], {})}
        #         ),
        #         beam.pvalue.AsDict(doc_counts)
        #     )
        # )


        )

if __name__ == '__main__':
    run_pipeline()