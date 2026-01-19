# python
import math
import os
import pathlib

import indexer
import pytest

def test__read_db_docs_as_text(monkeypatch):
    rows = [
        (1, "doc1.txt", "treść dokumentu 1", None),
        (2, "doc2.txt", None, None),
    ]
    monkeypatch.setattr(indexer, "fetch_all_documents", lambda: rows)
    result = list(indexer._read_db_docs_as_text())
    assert result == [("doc1.txt", "treść dokumentu 1"), ("doc2.txt", "")]

def test_run_pipeline_inserts(monkeypatch):
    rows = [
        (1, "doc1.txt", "abc", None),
        (2, "doc2.txt", None, None),
    ]
    monkeypatch.setattr(indexer, "fetch_all_documents", lambda: rows)

    inserted = []
    def fake_insert(path, content):
        inserted.append((path, content))
    monkeypatch.setattr(indexer, "insert_document", fake_insert)

    # zapobiegamy faktycznemu tworzeniu schematu w DB
    monkeypatch.setattr(indexer, "create_table", lambda: None)

    # Uruchamiamy pipeline; po jego zakończeniu powinny być wywołania insert_document
    indexer.run_pipeline()

    assert inserted == [("doc1.txt", "abc"), ("doc2.txt", "")]