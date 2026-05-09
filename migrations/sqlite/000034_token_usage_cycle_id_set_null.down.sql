-- Revert to the pre-000034 schema where cycle_id is a plain TEXT column
-- with no FK constraint. Same table-rebuild pattern as the up migration.

PRAGMA defer_foreign_keys = 1;

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
  cycle_id      TEXT
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
