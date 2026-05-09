-- token_usage.namespace_id is NOT NULL with no ON DELETE action, so the
-- project-delete cascade clears it explicitly via UPDATE ... WHERE namespace_id = ?
-- (internal/storage/token_usage_repo.go:ReassignNamespaceTx). Without this
-- index that statement scans the entire token_usage table on every project
-- delete, which is the dominant cost on installs with non-trivial usage
-- history (one row per LLM request).
CREATE INDEX IF NOT EXISTS idx_token_usage_namespace
  ON token_usage (namespace_id);
