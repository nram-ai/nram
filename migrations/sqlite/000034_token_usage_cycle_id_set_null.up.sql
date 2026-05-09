-- Postgres mirror lives at migrations/postgres/000031. SQLite cannot ALTER
-- a FK action in place, so token_usage is rebuilt via the same pattern as
-- migrations/sqlite/000023_memory_fk_cascade.up.sql. 000029 introduced
-- cycle_id as a bare TEXT column; this migration adds the FK with
-- ON DELETE SET NULL so both backends behave identically when the
-- project-delete cascade propagates through dream_cycles.
--
-- defer_foreign_keys lets the rebuild finish inside golang-migrate's
-- transaction without intermediate FK errors. The defensive UPDATE clears
-- any orphan cycle_id values that would fail the INSERT into the rebuilt
-- table.

PRAGMA defer_foreign_keys = 1;

UPDATE token_usage
   SET cycle_id = NULL
 WHERE cycle_id IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM dream_cycles dc WHERE dc.id = token_usage.cycle_id);

CREATE TABLE token_usage_new (
  id            TEXT PRIMARY KEY,
  org_id        TEXT REFERENCES organizations(id),
  user_id       TEXT REFERENCES users(id),
  project_id    TEXT REFERENCES projects(id),
  namespace_id  TEXT NOT NULL REFERENCES namespaces(id),
  operation     TEXT NOT NULL,
  provider      TEXT NOT NULL,
  model         TEXT NOT NULL,
  tokens_input  INTEGER NOT NULL DEFAULT 0,
  tokens_output INTEGER NOT NULL DEFAULT 0,
  memory_id     TEXT REFERENCES memories(id) ON DELETE SET NULL,
  api_key_id    TEXT REFERENCES api_keys(id),
  latency_ms    INTEGER,
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  success       INTEGER NOT NULL DEFAULT 1,
  error_code    TEXT,
  request_id    TEXT,
  cycle_id      TEXT REFERENCES dream_cycles(id) ON DELETE SET NULL
);

INSERT INTO token_usage_new (
  id, org_id, user_id, project_id, namespace_id, operation, provider, model,
  tokens_input, tokens_output, memory_id, api_key_id, latency_ms, created_at,
  success, error_code, request_id, cycle_id
)
SELECT
  id, org_id, user_id, project_id, namespace_id, operation, provider, model,
  tokens_input, tokens_output, memory_id, api_key_id, latency_ms, created_at,
  success, error_code, request_id, cycle_id
FROM token_usage;

DROP TABLE token_usage;
ALTER TABLE token_usage_new RENAME TO token_usage;

CREATE INDEX idx_token_usage_org_time ON token_usage (org_id, created_at);
CREATE INDEX idx_token_usage_user_time ON token_usage (user_id, created_at);
CREATE INDEX idx_token_usage_project_time ON token_usage (project_id, created_at);
CREATE INDEX idx_token_usage_operation ON token_usage (operation, created_at);
CREATE INDEX idx_token_usage_request_id ON token_usage (request_id);
CREATE INDEX idx_token_usage_success_time ON token_usage (success, created_at);
CREATE INDEX idx_token_usage_cycle_id ON token_usage (cycle_id) WHERE cycle_id IS NOT NULL;
