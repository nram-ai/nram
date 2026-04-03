ALTER TABLE memory_lineage ADD COLUMN namespace_id UUID REFERENCES namespaces(id);
UPDATE memory_lineage SET namespace_id = m.namespace_id FROM memories m WHERE m.id = memory_lineage.memory_id;
DELETE FROM memory_lineage WHERE namespace_id IS NULL;
ALTER TABLE memory_lineage ALTER COLUMN namespace_id SET NOT NULL;
CREATE INDEX idx_memory_lineage_namespace ON memory_lineage(namespace_id);

ALTER TABLE entity_aliases ADD COLUMN namespace_id UUID REFERENCES namespaces(id);
UPDATE entity_aliases SET namespace_id = e.namespace_id FROM entities e WHERE e.id = entity_aliases.entity_id;
DELETE FROM entity_aliases WHERE namespace_id IS NULL;
ALTER TABLE entity_aliases ALTER COLUMN namespace_id SET NOT NULL;
CREATE INDEX idx_entity_aliases_namespace ON entity_aliases(namespace_id);
