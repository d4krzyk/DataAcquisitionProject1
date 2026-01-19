import math
import os
import searcher


def approx_dict(a, b, rel_tol=1e-9, abs_tol=1e-12):
    # Pomocnicza funkcja porównująca słowniki z wartościami float.
    if set(a.keys()) != set(b.keys()):
        return False
    for k in a:
        if not math.isclose(a[k], b[k], rel_tol=rel_tol, abs_tol=abs_tol):
            return False
    return True



def test__build_qvec_local_simple():
    df = {"kot": 1, "pies": 2}
    N = 3
    query = "kot kot pies"

    qvec = searcher._build_query_vec(query, df, N)

    smooth = os.getenv('SMOOTH_IDF', '1') == '1'
    tokens = ["kot", "kot", "pies"]
    total = len(tokens)
    tf_expected = {"kot": 2 / total, "pies": 1 / total}
    expected = {}
    import math as _math
    for token, tfv in tf_expected.items():
        dfv = df.get(token, 1)
        if smooth:
            idf = _math.log((1 + N) / (1 + dfv)) + 1.0
        else:
            idf = _math.log(N / dfv) if dfv > 0 else 0.0
        expected[token] = tfv * idf

    assert approx_dict(qvec, expected)


def test_cosine_similarity_basic():
    a = {"x": 1.0, "y": 2.0}
    b = {"x": 2.0, "y": 0.0}

    expected = 2.0 / (math.sqrt(5.0) * 2.0)
    got = searcher.cosine_similarity(a, b)
    assert math.isclose(got, expected, rel_tol=1e-9)


def test_score_transformations_and_scoring():
    df = {"a": 1, "b": 1}
    N = 2
    query = "a b"

    # dokument w formacie używanym w searcher (_read_docs_worker)
    doc = {"db_id": 42, "path": "doc.txt", "tf": {"a": 0.6, "b": 0.8}}

    qvec = searcher._build_query_vec(query, df, N)
    doc_tfidf = searcher._tf_to_tfidf(doc["tf"], df, N)
    expected_score = searcher.cosine_similarity(qvec, doc_tfidf)

    # sprawdzamy strukturę oraz wynik podobieństwa
    assert isinstance(qvec, dict)
    assert isinstance(doc_tfidf, dict)
    got_score = searcher.cosine_similarity(qvec, doc_tfidf)
    assert math.isclose(got_score, expected_score, rel_tol=1e-9)
