# language: python
# File: app/database.py
import os
import json
import time
import psycopg

def _conn():
    return psycopg.connect(
        host=os.getenv("DATABASE_HOST", "localhost"),
        port=int(os.getenv("DATABASE_PORT", 5432)),
        dbname=os.getenv("DATABASE_NAME", "indexer_db"),
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
                        filename TEXT NOT NULL,
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
    data = index_structure if isinstance(index_structure, str) else json.dumps(index_structure)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO files (filename, index_structure) VALUES (%s, %s)",
                (filename, data)
            )

def fetch_all_documents():
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, filename, index_structure FROM files")
            return cur.fetchall()