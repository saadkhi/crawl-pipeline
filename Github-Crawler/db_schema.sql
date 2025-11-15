-- db_schema.sql
CREATE TABLE IF NOT EXISTS repos (
  id              TEXT PRIMARY KEY,     -- GitHub nodeId (global)
  owner           TEXT NOT NULL,
  name            TEXT NOT NULL,
  full_name       TEXT NOT NULL,
  url             TEXT,
  default_branch  TEXT,
  description     TEXT,
  language        TEXT,
  updated_at      TIMESTAMPTZ,
  first_seen_at   TIMESTAMPTZ DEFAULT now(),
  last_seen_at    TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_repos_fullname ON repos (full_name);

CREATE TABLE IF NOT EXISTS repo_stars (
  id            BIGSERIAL PRIMARY KEY,
  repo_id       TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
  observed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  stargazers    INTEGER NOT NULL,
  UNIQUE(repo_id, observed_at)
);

-- Flexible JSON metadata (when you need extra fields quickly)
CREATE TABLE IF NOT EXISTS repo_meta (
  repo_id     TEXT PRIMARY KEY REFERENCES repos(id) ON DELETE CASCADE,
  meta        JSONB,
  updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Simple table to keep crawl progress (cursor)
CREATE TABLE IF NOT EXISTS crawl_progress (
  id         TEXT PRIMARY KEY,
  cursor     TEXT,
  last_run   TIMESTAMPTZ DEFAULT now()
);
