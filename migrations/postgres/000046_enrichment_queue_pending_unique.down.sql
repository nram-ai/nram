-- Drop the partial unique index. The duplicate pending rows deleted by the up
-- migration cannot be reconstructed; that part is one-way (a rollback must not
-- error), mirroring the 000045 precedent.
DROP INDEX IF EXISTS idx_enrichment_queue_pending_memory;
