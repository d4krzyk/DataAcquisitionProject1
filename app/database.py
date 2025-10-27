# language: python
# File: app/database.py
import os
import json
import time
import psycopg

def _conn():
    return psycopg.connect(
        host=os.getenv("DATABASE_HOST", "127.0.0.1"),
        port=int(os.getenv("DATABASE_PORT", 5433)),
        dbname=os.getenv("DATABASE_NAME", "index_searcher_db"),
        user=os.getenv("DATABASE_USER", "a_user"),
        password=os.getenv("DATABASE_PASSWORD", "pass123"),
    )

def create_table(retries=10, delay=1):
    for attempt in range(retries):
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    CREATE TABLE IF NOT EXISTS files (
                        id SERIAL PRIMARY KEY,
                        filename TEXT NOT NULL UNIQUE,
                        index_structure JSONB NOT NULL
                    )
                    """)
            return
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(delay)

def insert_document(filename, index_structure):
    # index_structure może być dict lub już json string
    new_json = index_structure if isinstance(index_structure, str) else json.dumps(index_structure)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT index_structure FROM files WHERE filename = %s", (filename,))
            row = cur.fetchone()
            if row:
                existing = row[0]
                # row[0] może być już dict (psycopg może zwrócić deserializowany JSON) albo string
                if isinstance(existing, (dict, list)):
                    existing_json = json.dumps(existing, sort_keys=True)
                else:
                    existing_json = json.dumps(json.loads(existing), sort_keys=True) if isinstance(existing,
                                                                                                   str) else str(
                        existing)
                if existing_json == new_json:
                    # Nic do zrobienia
                    return
                # Aktualizuj tylko gdy zawartość się zmieniła
                cur.execute(
                    "UPDATE files SET index_structure = %s WHERE filename = %s",
                    (new_json, filename)
                )
            else:
                cur.execute(
                    "INSERT INTO files (filename, index_structure) VALUES (%s, %s)",
                    (filename, new_json)
                )

def fetch_all_documents():
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, filename, index_structure FROM files")
            return cur.fetchall()