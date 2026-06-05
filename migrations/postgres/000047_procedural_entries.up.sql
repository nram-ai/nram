-- Procedural memory tier: verbatim standing instructions / operating rules,
-- scoped to a user's root namespace. Distinct from memories: never embedded,
-- enriched, consolidated, or surfaced by recall. Fetched whole, verbatim.
CREATE TABLE procedural_entries (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id  UUID NOT NULL REFERENCES namespaces(id),
  content       TEXT NOT NULL,
  title         TEXT DEFAULT '',
  category      TEXT DEFAULT '',
  tags          TEXT[] DEFAULT '{}',
  priority      INT DEFAULT 0,
  enabled       BOOLEAN DEFAULT true,
  origin        TEXT DEFAULT 'user',
  metadata      JSONB DEFAULT '{}',
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now(),
  deleted_at    TIMESTAMPTZ
);

CREATE INDEX idx_procedural_namespace
  ON procedural_entries (namespace_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_procedural_order
  ON procedural_entries (namespace_id, priority DESC, created_at DESC) WHERE deleted_at IS NULL;
