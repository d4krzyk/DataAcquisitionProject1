# DataAcquisitionProject1

Prosty pipeline indeksujący i wyszukiwający dokumenty tekstowe i zapisujący strukturę indeksu do PostgreSQL. Używa Apache Beam do przetwarzania, psycopg do połączeń z bazą oraz Dockera do uruchomienia środowiska.

## Struktura projektu
- `Dockerfile` — obraz aplikacji Python.
- `docker-compose.yml` — usługi: `postgres` i `app`.
- `init.sql` — inicjalizacja bazy (tworzenie tabeli `files`).
- `requirements.txt` — zależności Pythona.
- `app/`
  - `indexer.py` — pipeline TF‑IDF + zapis do DB.
  - `database.py` — helper do Postgresa (create_table, insert_document, fetch_all_documents).
- `docs/` — przykładowe dokumenty (\`doc1.txt\` ...).
- `tests/` — testy (uruchom `pytest`).

## Wymagania
- Docker i Docker Compose
- Python 3.11 (do uruchomienia lokalnie bez Dockera)

## Uruchomienie (Docker)
1. Zbudować i uruchomić:
   - `docker compose up --build`
2. Wyłączyć i usunąć wolumeny danych:
   - `docker compose down -v`
3. Odpalenie wyszukiwarki:
   - `docker-compose run --rm app python /app/searcher.py`

## Zmienne środowiskowe
Aplikacja odczytuje (przykładowe wartości są w \`docker-compose.yml\`):
- `DATABASE_HOST`
- `DATABASE_PORT`
- `DATABASE_NAME`
- `DATABASE_USER`
- `DATABASE_PASSWORD`

## Testy
- Uruchom: `pytest`

## Uwagi i troubleshooting
- `init.sql` tworzy tabelę `files` z kolumną `index_structure JSONB`.  
- `app/database.py` zawiera prosty retry w `create_table()` — przy pracy z Compose poprawia start przy braku gotowej bazy.  
- Montowanie wolumenów: montując `./app:/app` nadpisujesz pliki z obrazu — oczekiwane w trybie deweloperskim.  
- Jeśli instalacja z `requirements.txt` się nie powiedzie, sprawdź kodowanie pliku / usuń niewidoczne znaki i spróbuj ponownie.

Licencja i dalsze instrukcje można dopisać według potrzeb.