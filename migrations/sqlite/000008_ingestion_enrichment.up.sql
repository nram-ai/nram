-- Ingestion log table (raw input tracking).
CREATE TABLE ingestion_log (
  id            TEXT PRIMARY KEY,
  namespace_id  TEXT NOT NULL REFERENCES namespaces(id),
  source        TEXT NOT NULL,
  content_hash  TEXT,
  raw_content   TEXT NOT NULL,
  memory_ids    TEXT DEFAULT '[]',
  status        TEXT NOT NULL DEFAULT 'pending',
  error         TEXT,
  metadata      TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX idx_ingestion_log_namespace ON ingestion_log (namespace_id);
CREATE INDEX idx_ingestion_log_status ON ingestion_log (status);
CREATE INDEX idx_ingestion_log_content_hash ON ingestion_log (content_hash);

-- Enrichment queue table (pessimistic locking, per-org fair scheduling).
CREATE TABLE enrichment_queue (
  id              TEXT PRIMARY KEY,
  memory_id       TEXT NOT NULL REFERENCES memories(id),
  namespace_id    TEXT NOT NULL REFERENCES namespaces(id),
  status          TEXT NOT NULL DEFAULT 'pending',
  priority        INTEGER DEFAULT 0,
  claimed_at      TEXT,
  claimed_by      TEXT,
  attempts        INTEGER DEFAULT 0,
  max_attempts    INTEGER DEFAULT 3,
  last_error      TEXT,
  steps_completed TEXT DEFAULT '[]',
  completed_at    TEXT,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX idx_enrichment_queue_namespace ON enrichment_queue (namespace_id);
CREATE INDEX idx_enrichment_queue_memory ON enrichment_queue (memory_id);
CREATE INDEX idx_enrichment_queue_status ON enrichment_queue (status);
