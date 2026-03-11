-- Entities table (knowledge graph nodes).
CREATE TABLE entities (
  id            TEXT PRIMARY KEY,
  namespace_id  TEXT NOT NULL REFERENCES namespaces(id),
  name          TEXT NOT NULL,
  canonical     TEXT NOT NULL,
  entity_type   TEXT NOT NULL,
  embedding_dim INTEGER,
  properties    TEXT DEFAULT '{}',
  mention_count INTEGER DEFAULT 1,
  metadata      TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(namespace_id, canonical, entity_type)
);

CREATE INDEX idx_entities_namespace ON entities (namespace_id);
CREATE INDEX idx_entities_type ON entities (namespace_id, entity_type);

-- Relationships table (knowledge graph edges).
CREATE TABLE relationships (
  id              TEXT PRIMARY KEY,
  namespace_id    TEXT NOT NULL REFERENCES namespaces(id),
  source_id       TEXT NOT NULL REFERENCES entities(id),
  target_id       TEXT NOT NULL REFERENCES entities(id),
  relation        TEXT NOT NULL,
  weight          REAL DEFAULT 1.0,
  properties      TEXT DEFAULT '{}',
  valid_from      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  valid_until     TEXT,
  source_memory   TEXT REFERENCES memories(id),
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(namespace_id, source_id, target_id, relation, valid_from)
);

CREATE INDEX idx_relationships_source ON relationships (source_id);
CREATE INDEX idx_relationships_target ON relationships (target_id);
CREATE INDEX idx_relationships_namespace ON relationships (namespace_id);

-- Entity aliases table (coreference resolution).
CREATE TABLE entity_aliases (
  id            TEXT PRIMARY KEY,
  entity_id     TEXT NOT NULL REFERENCES entities(id),
  alias         TEXT NOT NULL,
  alias_type    TEXT DEFAULT 'name',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(entity_id, alias)
);

CREATE INDEX idx_aliases_alias ON entity_aliases (alias);

-- Memory lineage table (evolution tracking).
CREATE TABLE memory_lineage (
  id            TEXT PRIMARY KEY,
  memory_id     TEXT NOT NULL REFERENCES memories(id),
  parent_id     TEXT REFERENCES memories(id),
  relation      TEXT NOT NULL,
  context       TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
