-- Add namespace_id to memory_lineage, backfill from memories table.
ALTER TABLE memory_lineage ADD COLUMN namespace_id TEXT REFERENCES namespaces(id);
UPDATE memory_lineage SET namespace_id = (SELECT namespace_id FROM memories WHERE memories.id = memory_lineage.memory_id);
DELETE FROM memory_lineage WHERE namespace_id IS NULL;
CREATE INDEX idx_memory_lineage_namespace ON memory_lineage(namespace_id);

-- Add namespace_id to entity_aliases, backfill from entities table.
ALTER TABLE entity_aliases ADD COLUMN namespace_id TEXT REFERENCES namespaces(id);
UPDATE entity_aliases SET namespace_id = (SELECT namespace_id FROM entities WHERE entities.id = entity_aliases.entity_id);
DELETE FROM entity_aliases WHERE namespace_id IS NULL;
CREATE INDEX idx_entity_aliases_namespace ON entity_aliases(namespace_id);
