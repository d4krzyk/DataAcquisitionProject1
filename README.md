# DataAcquisitionProject1

Prosty pipeline indeksujący i wyszukujący dokumenty tekstowe. Indeks (TF‑IDF) zapisuje strukturę do PostgreSQL. Do przetwarzania używa Apache Beam, do komunikacji z bazą — `psycopg`.

## Zawartość repozytorium

- `Dockerfile` — buduje obraz aplikacji (Python).
- `docker-compose.yml` — uruchamia usługi: `postgres`, `redis` i kontenery aplikacji.
- `requirements.txt` — zależności Python.
- `init.sql` — inicjalizacja schematu bazy danych.
- `app/` — kod aplikacji:
  - `crawler.py` — prosty crawler zapisujący tekst i linki stron do bazy.
  - `indexer.py` — pipeline (Apache Beam) normalizujący i upsertujący dokumenty.
  - `searcher.py` — interaktywna wyszukiwarka TF‑IDF.
  - `database.py` — helpery do PostgreSQL (`create_table`, `insert_document`, `fetch_all_documents`).
- `tests/` — testy jednostkowe (pytest).

## Wymagania
- Docker i Docker Compose.
- Python 3.11.

## Uruchomienie (Docker)
1. Zbuduj i uruchom wszystkie serwisy:
   - `docker compose up --build`
2. W kontenerze aplikacji można uruchomić interaktywną wyszukiwarkę:
   - `docker compose run --rm searcher`
3. Zatrzymanie i usunięcie wolumenów danych:
   - `docker compose down -v`

## Zmienne środowiskowe

Ustaw w `docker-compose.yml` lub w środowisku uruchomieniowym.

Najważniejsze:
- `DATABASE_HOST` — host PostgreSQL (np. `postgres`)
- `DATABASE_PORT` — port PostgreSQL (np. `5432`)
- `DATABASE_NAME` — nazwa bazy (`index_searcher_db`)
- `DATABASE_USER` — użytkownik DB
- `DATABASE_PASSWORD` — hasło DB
- `SMOOTH_IDF` — `1` = wygładzone IDF (opcjonalne)
- `PYTHONWARNINGS` — np. `"ignore::DeprecationWarning"` (opcjonalne, do testów)

Opcjonalne / crawler / rate:
- `CRAWLER_START_URL`, `CRAWLER_MAX_DEPTH`, `CRAWLER_DELAY`, `CRAWLER_ALLOWED_DOMAIN`
- `CRAWLER_USER_AGENT`
- `CRAWLER_REDIS_URL`, `RATE_PER_SEC`, `RATE_KEY`
- `PYTHONPATH` — np. `/app`

Przykład (fragment `docker-compose.yml`):
```yaml
environment:
  DATABASE_HOST: postgres
  DATABASE_PORT: 5432
  DATABASE_NAME: index_searcher_db
  DATABASE_USER: a_user
  DATABASE_PASSWORD: pass123
  SMOOTH_IDF: "1"
  PYTHONWARNINGS: "ignore::DeprecationWarning"


