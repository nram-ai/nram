-- ON DELETE actions on FKs into memories(id). Choice per FK reflects whether
-- the child row carries independent value after the parent memory is gone:
--   token_usage           SET NULL  preserve billing/analytics history
--   memory_lineage.memory_id   CASCADE   lineage row is meaningless without parent
--   memory_lineage.parent_id   SET NULL  preserve child rows when only the parent is purged
--   enrichment_queue      CASCADE   queue item has no purpose without its memory
--   relationships.source_memory SET NULL  preserve graph edge; drop provenance pointer
--   memories.superseded_by SET NULL preserve historical supersession chain link

-- ADD CONSTRAINT … NOT VALID skips the validating scan and only takes a
-- brief metadata lock; VALIDATE CONSTRAINT then takes SHARE UPDATE EXCLUSIVE
-- (does not block DML). Existing rows already satisfy these FKs, so the
-- two-phase pattern is purely a lock-duration optimization.

ALTER TABLE token_usage
  DROP CONSTRAINT IF EXISTS token_usage_memory_id_fkey;
ALTER TABLE token_usage
  ADD CONSTRAINT token_usage_memory_id_fkey
    FOREIGN KEY (memory_id) REFERENCES memories(id) ON DELETE SET NULL NOT VALID;
ALTER TABLE token_usage VALIDATE CONSTRAINT token_usage_memory_id_fkey;

ALTER TABLE memory_lineage
  DROP CONSTRAINT IF EXISTS memory_lineage_memory_id_fkey;
ALTER TABLE memory_lineage
  ADD CONSTRAINT memory_lineage_memory_id_fkey
    FOREIGN KEY (memory_id) REFERENCES memories(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE memory_lineage VALIDATE CONSTRAINT memory_lineage_memory_id_fkey;

ALTER TABLE memory_lineage
  DROP CONSTRAINT IF EXISTS memory_lineage_parent_id_fkey;
ALTER TABLE memory_lineage
  ADD CONSTRAINT memory_lineage_parent_id_fkey
    FOREIGN KEY (parent_id) REFERENCES memories(id) ON DELETE SET NULL NOT VALID;
ALTER TABLE memory_lineage VALIDATE CONSTRAINT memory_lineage_parent_id_fkey;

ALTER TABLE enrichment_queue
  DROP CONSTRAINT IF EXISTS enrichment_queue_memory_id_fkey;
ALTER TABLE enrichment_queue
  ADD CONSTRAINT enrichment_queue_memory_id_fkey
    FOREIGN KEY (memory_id) REFERENCES memories(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE enrichment_queue VALIDATE CONSTRAINT enrichment_queue_memory_id_fkey;

ALTER TABLE relationships
  DROP CONSTRAINT IF EXISTS relationships_source_memory_fkey;
ALTER TABLE relationships
  ADD CONSTRAINT relationships_source_memory_fkey
    FOREIGN KEY (source_memory) REFERENCES memories(id) ON DELETE SET NULL NOT VALID;
ALTER TABLE relationships VALIDATE CONSTRAINT relationships_source_memory_fkey;

ALTER TABLE memories
  DROP CONSTRAINT IF EXISTS memories_superseded_by_fkey;
ALTER TABLE memories
  ADD CONSTRAINT memories_superseded_by_fkey
    FOREIGN KEY (superseded_by) REFERENCES memories(id) ON DELETE SET NULL NOT VALID;
ALTER TABLE memories VALIDATE CONSTRAINT memories_superseded_by_fkey;
