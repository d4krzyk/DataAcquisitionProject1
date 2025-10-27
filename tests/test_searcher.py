import math
import os

import searcher


def approx_dict(a, b, rel_tol=1e-9, abs_tol=1e-12):
    """Pomocnicza funkcja porównująca słowniki z wartościami float."""
    if set(a.keys()) != set(b.keys()):
        return False
    for k in a:
        if not math.isclose(a[k], b[k], rel_tol=rel_tol, abs_tol=abs_tol):
            return False
    return True


def test__build_qvec_local_simple():
    # Przygotuj prosty DF i liczbę dokumentów
    df = {"kot": 1, "pies": 2}
    N = 3
    query = "kot kot pies"

    # Zbuduj qvec
    qvec = searcher._build_qvec_local(query, df, N)

    # Ręcznie policz oczekiwane wartości (SMOOTH_IDF domyślnie włączony)
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
    # dot = 1*2 + 2*0 = 2
    # norm_a = sqrt(1+4) = sqrt(5)
    # norm_b = sqrt(4+0) = 2
    expected = 2.0 / (math.sqrt(5.0) * 2.0)
    got = searcher.cosine_similarity(a, b)
    assert math.isclose(got, expected, rel_tol=1e-9)


def test_score_doc_worker_and_cache(monkeypatch):
    # Upewnij się, że cache jest pusty na początku testu
    orig_cache = dict(searcher._qvec_cache)
    try:
        searcher._qvec_cache.clear()

        df = {"a": 1, "b": 1}
        N = 2
        query = "a b"

        # przygotuj dokument z tfidf (może być dowolny wektor)
        doc = {"db_id": 42, "filename": "doc.txt", "tfidf": {"a": 0.6, "b": 0.8}}

        # policz oczekiwany qvec ręcznie
        qvec = searcher._build_qvec_local(query, df, N)
        expected_score = searcher.cosine_similarity(qvec, doc["tfidf"])

        res = searcher.score_doc_worker(doc, df, N, query, sim=None)
        assert isinstance(res, tuple) and res[0] == 42 and res[1] == "doc.txt"
        assert math.isclose(res[2], expected_score, rel_tol=1e-9)

        # Cache powinien zawierać wektor zapytania teraz
        assert query in searcher._qvec_cache
        assert approx_dict(searcher._qvec_cache[query], qvec)

        # Wywołanie ponowne powinno korzystać z cache (nie zmienia wyniku)
        res2 = searcher.score_doc_worker(doc, df, N, query, sim=None)
        assert math.isclose(res2[2], expected_score, rel_tol=1e-9)
    finally:
        # przywróć oryginalny cache, by nie wpływać na inne testy
        searcher._qvec_cache.clear()
        searcher._qvec_cache.update(orig_cache)

