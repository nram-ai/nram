-- Adds two columns recording multi-vector facet state per memory:
--   faceted_at    When the memory was last processed by the facet pass
--   facet_count   Number of facets the pass produced (1 = single topic /
--                 facet 0 only, N = facet 0 plus N-1 topic facets)
-- Both NULL means "never faceted"; the facet backfill candidate query uses
-- faceted_at IS NULL to skip already-faceted memories, and the enrichment
-- monitor surfaces facet_count on the job detail view.
ALTER TABLE memories ADD COLUMN faceted_at TIMESTAMPTZ;
ALTER TABLE memories ADD COLUMN facet_count INTEGER;

-- Index predicate matches the WHERE in
-- MemoryRepo.ListMultiVectorBackfillCandidates exactly so the candidate sweep
-- is index-resolved instead of a full-table scan. The planner only chooses the
-- partial index when the query's WHERE is a logical superset of the predicate.
DROP INDEX IF EXISTS idx_memories_facet_backfill;
CREATE INDEX idx_memories_facet_backfill
  ON memories (namespace_id, created_at)
  WHERE faceted_at IS NULL
    AND deleted_at IS NULL
    AND superseded_by IS NULL;
