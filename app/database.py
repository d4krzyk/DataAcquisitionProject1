# app/database.py
import os
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


def create_table(retries: int = 10, delay: float = 1.0):
    """
    Zapewnia schemat:
    files(id serial pk, path text unique, content text, created_at timestamptz default now()).

    Obsługuje istniejący wolumen z wcześniejszym schematem:
    - tworzy tabelę jeśli nie istnieje
    - dodaje brakujące kolumny
    - dodaje brakujący unikalny indeks/constraint na path
    """
    for attempt in range(retries):
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    # Tabela (minimum)
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS files (
                          id SERIAL PRIMARY KEY
                        );
                        """
                    )

                    # Kolumny (migracja bezpieczna)
                    cur.execute("ALTER TABLE files ADD COLUMN IF NOT EXISTS path TEXT;")
                    cur.execute("ALTER TABLE files ADD COLUMN IF NOT EXISTS content TEXT;")
                    cur.execute(
                        "ALTER TABLE files ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now();"
                    )

                    # Unikalność path (jako indeks, działa nawet jeśli constraint nie istniał)
                    cur.execute(
                        """
                        DO $$
                        BEGIN
                          IF NOT EXISTS (
                            SELECT 1
                            FROM pg_indexes
                            WHERE schemaname = current_schema()
                              AND tablename = 'files'
                              AND indexname = 'files_path_key'
                          ) THEN
                            CREATE UNIQUE INDEX files_path_key ON files(path);
                          END IF;
                        END $$;
                        """
                    )
            return
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(delay)


def insert_document(path: str, content: str):
    if content is None:
        content = ""

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO files (path, content)
                VALUES (%s, %s)
                ON CONFLICT (path)
                DO UPDATE SET
                    content = EXCLUDED.content,
                    created_at = now();
                """,
                (path, content),
            )


def fetch_all_documents():
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, path, content, created_at FROM files ORDER BY id;")
            return cur.fetchall()
