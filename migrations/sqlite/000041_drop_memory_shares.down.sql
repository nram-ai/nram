CREATE TABLE IF NOT EXISTS memory_shares (
  id              TEXT PRIMARY KEY,
  source_ns_id    TEXT NOT NULL REFERENCES namespaces(id),
  target_ns_id    TEXT NOT NULL REFERENCES namespaces(id),
  permission      TEXT NOT NULL DEFAULT 'recall',
  created_by      TEXT REFERENCES users(id),
  expires_at      TEXT,
  revoked_at      TEXT,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(source_ns_id, target_ns_id)
);

CREATE INDEX IF NOT EXISTS idx_shares_target ON memory_shares (target_ns_id);
