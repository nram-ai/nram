-- Procedural memory tier: verbatim standing instructions / operating rules,
-- scoped to a user's root namespace. Distinct from memories: never embedded,
-- enriched, consolidated, or surfaced by recall. Fetched whole, verbatim.
CREATE TABLE procedural_entries (
  id            TEXT PRIMARY KEY,
  namespace_id  TEXT NOT NULL REFERENCES namespaces(id),
  content       TEXT NOT NULL,
  title         TEXT DEFAULT '',
  category      TEXT DEFAULT '',
  tags          TEXT DEFAULT '[]',
  priority      INTEGER DEFAULT 0,
  enabled       INTEGER DEFAULT 1,
  origin        TEXT DEFAULT 'user',
  metadata      TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  deleted_at    TEXT
);

CREATE INDEX idx_procedural_namespace ON procedural_entries (namespace_id);
CREATE INDEX idx_procedural_order ON procedural_entries (namespace_id, priority DESC, created_at DESC);
