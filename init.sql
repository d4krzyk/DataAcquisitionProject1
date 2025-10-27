GRANT ALL PRIVILEGES ON DATABASE index_searcher_db TO a_user;

CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL UNIQUE,
    index_structure JSONB NOT NULL
);