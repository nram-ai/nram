-- Memories table.
CREATE TABLE memories (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id  UUID NOT NULL REFERENCES namespaces(id),
  content       TEXT NOT NULL,
  embedding_dim INT,
  source        TEXT,
  tags          TEXT[] DEFAULT '{}',
  confidence    FLOAT DEFAULT 1.0,
  importance    FLOAT DEFAULT 0.5,
  access_count  INT DEFAULT 0,
  last_accessed TIMESTAMPTZ,
  expires_at    TIMESTAMPTZ,
  superseded_by UUID REFERENCES memories(id),
  enriched      BOOLEAN DEFAULT false,
  metadata      JSONB DEFAULT '{}',
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now(),
  deleted_at    TIMESTAMPTZ,
  purge_after   TIMESTAMPTZ
);

CREATE INDEX idx_memories_namespace
  ON memories (namespace_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_memories_expires
  ON memories (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_memories_tags
  ON memories USING gin (tags);
CREATE INDEX idx_memories_enriched
  ON memories (namespace_id) WHERE enriched = false;
CREATE INDEX idx_memories_dim
  ON memories (embedding_dim) WHERE embedding_dim IS NOT NULL;
CREATE INDEX idx_memories_fulltext
  ON memories USING gin (to_tsvector('english', content));

-- System metadata table.
CREATE TABLE system_meta (
  key         TEXT PRIMARY KEY,
  value       TEXT NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT now(),
  updated_at  TIMESTAMPTZ DEFAULT now()
);
