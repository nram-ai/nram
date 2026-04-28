-- Revert ON DELETE actions on FKs into memories(id) back to the default
-- (NO ACTION). This restores the pre-000020 schema where hard-deleting a
-- memory with surviving children fails with SQLSTATE 23503.

ALTER TABLE token_usage
  DROP CONSTRAINT IF EXISTS token_usage_memory_id_fkey;
ALTER TABLE token_usage
  ADD CONSTRAINT token_usage_memory_id_fkey
    FOREIGN KEY (memory_id) REFERENCES memories(id);

ALTER TABLE memory_lineage
  DROP CONSTRAINT IF EXISTS memory_lineage_memory_id_fkey;
ALTER TABLE memory_lineage
  ADD CONSTRAINT memory_lineage_memory_id_fkey
    FOREIGN KEY (memory_id) REFERENCES memories(id);

ALTER TABLE memory_lineage
  DROP CONSTRAINT IF EXISTS memory_lineage_parent_id_fkey;
ALTER TABLE memory_lineage
  ADD CONSTRAINT memory_lineage_parent_id_fkey
    FOREIGN KEY (parent_id) REFERENCES memories(id);

ALTER TABLE enrichment_queue
  DROP CONSTRAINT IF EXISTS enrichment_queue_memory_id_fkey;
ALTER TABLE enrichment_queue
  ADD CONSTRAINT enrichment_queue_memory_id_fkey
    FOREIGN KEY (memory_id) REFERENCES memories(id);

ALTER TABLE relationships
  DROP CONSTRAINT IF EXISTS relationships_source_memory_fkey;
ALTER TABLE relationships
  ADD CONSTRAINT relationships_source_memory_fkey
    FOREIGN KEY (source_memory) REFERENCES memories(id);

ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_superseded_by_fkey;
ALTER TABLE memories
  ADD CONSTRAINT memories_superseded_by_fkey
    FOREIGN KEY (superseded_by) REFERENCES memories(id);
