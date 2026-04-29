-- Indexes covering the enrichment worker's "has prior work" probes added
-- alongside the per-step skip gate. Without these, every backfill job
-- runs two namespace-scoped scans (lineage + relationships) per memory,
-- which on a populated namespace is a full table walk filtered only by
-- namespace_id.
--
-- idx_memory_lineage_parent_relation: covers
--   SELECT 1 FROM memory_lineage WHERE namespace_id = $1 AND parent_id = $2
--     AND relation = $3 LIMIT 1
-- (the HasExtractedFactChildren probe). Also speeds up
-- FindChildIDsByRelation in the same repo.
CREATE INDEX IF NOT EXISTS idx_memory_lineage_parent_relation
  ON memory_lineage (namespace_id, parent_id, relation);

-- idx_relationships_source_memory: covers
--   SELECT 1 FROM relationships WHERE namespace_id = $1 AND source_memory = $2
--     LIMIT 1
-- (the HasBySourceMemory probe).
CREATE INDEX IF NOT EXISTS idx_relationships_source_memory
  ON relationships (namespace_id, source_memory);
