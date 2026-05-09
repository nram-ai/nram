-- Mirrors postgres 000032 (per-FK rationale lives there). SQLite cannot
-- ALTER a FK action in place, so each affected table is rebuilt via the
-- documented pattern: create _new with the new constraint, copy data, drop,
-- rename, re-create indexes. defer_foreign_keys = 1 lets the rebuild finish
-- inside golang-migrate's transaction without intermediate FK errors.
--
-- Tables rebuilt:
--   entity_aliases    entity_id        ON DELETE CASCADE
--   relationships     source_id        ON DELETE CASCADE
--                     target_id        ON DELETE CASCADE
--                     source_memory    ON DELETE SET NULL (preserved from 000023)
--
-- Schema includes columns and indexes added by all later migrations applied
-- against these tables: entity_aliases.namespace_id and idx_entity_aliases_namespace
-- from 000017, and idx_relationships_source_memory from 000024. If a future
-- migration touches these tables, this rebuild's SELECT and CREATE INDEX
-- blocks must also be updated.

PRAGMA defer_foreign_keys = 1;

-- entity_aliases rebuild: entity_id ON DELETE CASCADE.
CREATE TABLE entity_aliases_new (
  id            TEXT PRIMARY KEY,
  entity_id     TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
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

-- relationships rebuild: source_id and target_id ON DELETE CASCADE,
-- source_memory keeps ON DELETE SET NULL from 000023.
CREATE TABLE relationships_new (
  id              TEXT PRIMARY KEY,
  namespace_id    TEXT NOT NULL REFERENCES namespaces(id),
  source_id       TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  target_id       TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
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
