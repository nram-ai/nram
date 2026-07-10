-- Backs MemoryRepo.CountMissingEmbeddings, which counts live embeddable rows
-- awaiting a vector (embedding_dim IS NULL) to drive the enrichment health
-- surface. idx_memories_dim is WHERE embedding_dim IS NOT NULL (the opposite
-- predicate), so the NULL count could only seqscan the whole memories table.
-- This partial index holds just the live, non-superseded, unembedded rows (a
-- set that shrinks to near-zero as the backfill drains), so the count is
-- index-resolved. The planner only chooses a partial index when the query's
-- WHERE is a logical superset of the predicate; confidence > 0 and the content
-- checks remain residual filters.
DROP INDEX IF EXISTS idx_memories_missing_embedding;
CREATE INDEX idx_memories_missing_embedding
  ON memories (namespace_id)
  WHERE embedding_dim IS NULL
    AND deleted_at IS NULL
    AND superseded_by IS NULL;
