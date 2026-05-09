-- Revert ON DELETE actions on FKs into entities(id) to the default (NO ACTION).
-- Same table-rebuild pattern as the .up.sql, with the CASCADE clause omitted.

PRAGMA defer_foreign_keys = 1;

CREATE TABLE entity_aliases_new (
  id            TEXT PRIMARY KEY,
  entity_id     TEXT NOT NULL REFERENCES entities(id),
  alias         TEXT NOT NULL,
  alias_type    TEXT DEFAULT 'name',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  namespace_id  TEXT REFERENCES namespaces(id),
  UNIQUE(entity_id, alias)
);

INSERT INTO entity_aliases_new (id, entity_id, alias, alias_type, created_at, namespace_id)
SELECT id, entity_id, alias, alias_type, created_at, namespace_id FROM entity_aliases;

DROP TABLE entity_aliases;
ALTER TABLE entity_aliases_new RENAME TO entity_aliases;

CREATE INDEX idx_aliases_alias ON entity_aliases (alias);
CREATE INDEX idx_entity_aliases_namespace ON entity_aliases (namespace_id);

CREATE TABLE relationships_new (
  id              TEXT PRIMARY KEY,
  namespace_id    TEXT NOT NULL REFERENCES namespaces(id),
  source_id       TEXT NOT NULL REFERENCES entities(id),
  target_id       TEXT NOT NULL REFERENCES entities(id),
  relation        TEXT NOT NULL,
  weight          REAL DEFAULT 1.0,
  properties      TEXT DEFAULT '{}',
  valid_from      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  valid_until     TEXT,
  source_memory   TEXT REFERENCES memories(id) ON DELETE SET NULL,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(namespace_id, source_id, target_id, relation, valid_from)
);

INSERT INTO relationships_new (
  id, namespace_id, source_id, target_id, relation, weight, properties,
  valid_from, valid_until, source_memory, created_at
)
SELECT
  id, namespace_id, source_id, target_id, relation, weight, properties,
  valid_from, valid_until, source_memory, created_at
FROM relationships;

DROP TABLE relationships;
ALTER TABLE relationships_new RENAME TO relationships;

CREATE INDEX idx_relationships_source ON relationships (source_id);
CREATE INDEX idx_relationships_target ON relationships (target_id);
CREATE INDEX idx_relationships_namespace ON relationships (namespace_id);
CREATE INDEX idx_relationships_source_memory ON relationships (namespace_id, source_memory);
