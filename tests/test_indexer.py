# python
import math
import os
import pathlib

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

import pytest

import indexer


def test_tokenize_and_stopwords():
    text = "Ala ma kota i psa. To jest test."
    tokens = indexer.tokenize(text)
    assert "i" not in tokens
    assert "to" not in tokens
    assert "ala" in tokens
    assert "ma" in tokens


def test_compute_tf():
    tokens = ["a", "b", "a", "c"]
    tf = indexer.compute_tf(tokens)
    assert tf["a"] == pytest.approx(2 / 4)
    assert tf["b"] == pytest.approx(1 / 4)
    assert tf["c"] == pytest.approx(1 / 4)


def test_pipeline():
    docs = [
        ("doc1.txt", "kot pies kot"),
        ("doc2.txt", "pies ryba")
    ]
    # przygotuj oczekiwane wartości TF-IDF
    docs_tokens = {}
    for path, txt in docs:
        toks = indexer.tokenize(txt)
        docs_tokens[path] = toks
    N = len(docs)
    df = {}
    for toks in docs_tokens.values():
        for t in set(toks):
            df[t] = df.get(t, 0) + 1
    expected = {}
    for p, toks in docs_tokens.items():
        tf = indexer.compute_tf(toks)
        tfidf = {}
        for t, v in tf.items():
            dfv = df.get(t, 1)
            tfidf[t] = v * math.log(N / dfv)
        expected[p] = tfidf

    def check_fn(elem, df_dict, total_docs, expected_map):
        path, tf_dict = elem
        tfidf = {}
        for t, v in tf_dict.items():
            dfv = df_dict.get(t, 1)
            tfidf[t] = v * math.log(total_docs / dfv)
        exp = expected_map[path]
        if set(tfidf.keys()) != set(exp.keys()):
            return False
        for t, v in tfidf.items():
            if not math.isclose(v, exp[t], rel_tol=1e-6, abs_tol=1e-12):
                return False
        return True

    with BeamTestPipeline() as p:
        pcoll = p | beam.Create(docs)

        doc_tokens = pcoll | "Tokenize" >> beam.Map(lambda kv: (kv[0], indexer.tokenize(kv[1])))
        doc_tf = doc_tokens | "TF" >> beam.Map(lambda kv: (kv[0], indexer.compute_tf(kv[1])))
        token_doc_ones = doc_tokens | "UniqueTokens" >> beam.FlatMap(lambda kv: ((t, 1) for t in set(kv[1])))
        df_pcoll = token_doc_ones | "DF" >> beam.CombinePerKey(sum)

        checked = doc_tf | "CheckTFIDF" >> beam.Map(
            check_fn,
            beam.pvalue.AsDict(df_pcoll),
            N,
            expected
        )

        # oczekujemy po jednym True dla każdego dokumentu
        assert_that(checked, equal_to([True] * N))