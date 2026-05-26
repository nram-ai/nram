CREATE TABLE IF NOT EXISTS memory_shares (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_ns_id    UUID NOT NULL REFERENCES namespaces(id),
  target_ns_id    UUID NOT NULL REFERENCES namespaces(id),
  permission      TEXT NOT NULL DEFAULT 'recall',
  created_by      UUID REFERENCES users(id),
  expires_at      TIMESTAMPTZ,
  revoked_at      TIMESTAMPTZ,
  created_at      TIMESTAMPTZ DEFAULT now(),
  UNIQUE(source_ns_id, target_ns_id)
);

CREATE INDEX IF NOT EXISTS idx_shares_target ON memory_shares (target_ns_id);
