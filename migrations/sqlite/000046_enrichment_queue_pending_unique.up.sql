-- Enforce at most one UNCLAIMED-pending enrichment job per memory. A claimed
-- job is status='processing' (ClaimNext flips it), so this still allows a fresh
-- pending row to coexist with an in-flight one -- which is correct when a
-- memory's content is edited while its prior job is mid-flight. Without this,
-- repeated enqueues (manual augmentation backfill, the dream-cycle augmentation
-- backfill phase, rapid re-stores) pile up duplicate pending rows that the
-- worker processes redundantly.
--
-- First collapse any existing duplicates so the unique index can build: keep
-- the earliest pending row per memory_id (by created_at, then id) and delete
-- the rest. The outer table is referenced by name (not an alias) so the
-- correlated subquery is portable to SQLite, which cannot alias a DELETE target.
DELETE FROM enrichment_queue
WHERE status = 'pending'
  AND EXISTS (
    SELECT 1 FROM enrichment_queue o
    WHERE o.memory_id = enrichment_queue.memory_id
      AND o.status = 'pending'
      AND (o.created_at < enrichment_queue.created_at
           OR (o.created_at = enrichment_queue.created_at AND o.id < enrichment_queue.id))
  );

CREATE UNIQUE INDEX idx_enrichment_queue_pending_memory
  ON enrichment_queue (memory_id) WHERE status = 'pending';
