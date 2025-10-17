GRANT ALL PRIVILEGES ON DATABASE indexer_db TO a_user;

CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    index_structure JSONB NOT NULL
);