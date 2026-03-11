-- Ingestion log table (raw input tracking).
CREATE TABLE ingestion_log (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id  UUID NOT NULL REFERENCES namespaces(id),
  source        TEXT NOT NULL,
  content_hash  TEXT,
  raw_content   TEXT NOT NULL,
  memory_ids    UUID[] DEFAULT '{}',
  status        TEXT NOT NULL DEFAULT 'pending',
  error         JSONB,
  metadata      JSONB DEFAULT '{}',
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_ingestion_log_namespace ON ingestion_log (namespace_id);
CREATE INDEX idx_ingestion_log_status ON ingestion_log (status);
CREATE INDEX idx_ingestion_log_content_hash ON ingestion_log (content_hash);

-- Enrichment queue table (pessimistic locking, per-org fair scheduling).
CREATE TABLE enrichment_queue (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  memory_id       UUID NOT NULL REFERENCES memories(id),
  namespace_id    UUID NOT NULL REFERENCES namespaces(id),
  status          TEXT NOT NULL DEFAULT 'pending',
  priority        INT DEFAULT 0,
  claimed_at      TIMESTAMPTZ,
  claimed_by      TEXT,
  attempts        INT DEFAULT 0,
  max_attempts    INT DEFAULT 3,
  last_error      JSONB,
  steps_completed JSONB DEFAULT '[]',
  completed_at    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_enrichment_queue_namespace ON enrichment_queue (namespace_id);
CREATE INDEX idx_enrichment_queue_memory ON enrichment_queue (memory_id);
CREATE INDEX idx_enrichment_queue_status ON enrichment_queue (status);
CREATE INDEX idx_enrichment_queue_pending
  ON enrichment_queue (namespace_id, priority DESC, created_at) WHERE status = 'pending';
CREATE INDEX idx_enrichment_queue_stale
  ON enrichment_queue (status, claimed_at);
