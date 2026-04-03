-- Drop indexes first
DROP INDEX IF EXISTS idx_memory_lineage_namespace;
DROP INDEX IF EXISTS idx_entity_aliases_namespace;

-- Recreate memory_lineage without namespace_id
CREATE TABLE memory_lineage_backup AS SELECT id, memory_id, parent_id, relation, context, created_at FROM memory_lineage;
DROP TABLE memory_lineage;
CREATE TABLE memory_lineage (
    id TEXT PRIMARY KEY,
    memory_id TEXT NOT NULL REFERENCES memories(id),
    parent_id TEXT REFERENCES memories(id),
    relation TEXT NOT NULL,
    context TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
INSERT INTO memory_lineage SELECT * FROM memory_lineage_backup;
DROP TABLE memory_lineage_backup;

-- Recreate entity_aliases without namespace_id
CREATE TABLE entity_aliases_backup AS SELECT id, entity_id, alias, alias_type, created_at FROM entity_aliases;
DROP TABLE entity_aliases;
CREATE TABLE entity_aliases (
    id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL REFERENCES entities(id),
    alias TEXT NOT NULL,
    alias_type TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
INSERT INTO entity_aliases SELECT * FROM entity_aliases_backup;
DROP TABLE entity_aliases_backup;
