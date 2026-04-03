DROP INDEX IF EXISTS idx_memory_lineage_namespace;
ALTER TABLE memory_lineage DROP COLUMN namespace_id;

DROP INDEX IF EXISTS idx_entity_aliases_namespace;
ALTER TABLE entity_aliases DROP COLUMN namespace_id;
