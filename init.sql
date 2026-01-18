GRANT ALL PRIVILEGES ON DATABASE index_searcher_db TO a_user;

CREATE TABLE IF NOT EXISTS files (
  id SERIAL PRIMARY KEY,
  path TEXT UNIQUE,
  content TEXT,
  created_at TIMESTAMP DEFAULT now()
);