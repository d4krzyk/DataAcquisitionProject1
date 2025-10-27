# DataAcquisitionProject1

Prosty pipeline indeksujący i wyszukujący dokumenty tekstowe. Indeks (TF‑IDF) zapisuje strukturę do PostgreSQL. Do przetwarzania używa Apache Beam, do komunikacji z bazą — `psycopg`.

## Zawartość repozytorium
- `Dockerfile` — obraz aplikacji Python.
- `docker-compose.yml` — usługi: `postgres` i `app`.
- `init.sql` — inicjalizacja bazy (tabela `files`).
- `requirements.txt` — zależności Python.
- `app/`
  - `indexer.py` — pipeline TF‑IDF + zapis do DB.
  - `database.py` — helpery do Postgresa (`create_table`, `insert_document`, `fetch_all_documents`).
  - `searcher.py` — interaktywne wyszukiwanie po TF‑IDF.
  - `docs/` — przykładowe dokumenty.
- `tests/` — testy jednostkowe.

## Wymagania
- Docker i Docker Compose (zalecane).
- Python 3.11 (do uruchomienia lokalnego bez Dockera).

## Uruchomienie (Docker)
1. Zbuduj i uruchom wszystkie serwisy:
   - `docker compose up --build`
2. W kontenerze aplikacji można uruchomić interaktywną wyszukiwarkę:
   - `docker compose run --rm app python /app/searcher.py`
3. Zatrzymanie i usunięcie wolumenów danych:
   - `docker compose down -v`

## Zmienne środowiskowe
Ustaw w `docker-compose.yml` lub w środowisku (przykład):
- `DATABASE_HOST`
- `DATABASE_PORT`
- `DATABASE_NAME`
- `DATABASE_USER`
- `DATABASE_PASSWORD`
- `SMOOTH_IDF` (opcjonalne; `1` = włączone)

