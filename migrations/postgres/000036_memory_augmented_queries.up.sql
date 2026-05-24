-- Adds two columns recording embedding-augmentation state per memory:
--   augmented_queries        JSONB array of paraphrase queries fed into the embedder
--   augmented_embedding_at   When the augmented embedding was last written
-- Both NULL means "embedded with raw content"; the partial index supports the
-- backfill query that finds memories needing augmentation.
ALTER TABLE memories ADD COLUMN augmented_queries JSONB;
ALTER TABLE memories ADD COLUMN augmented_embedding_at TIMESTAMPTZ;

-- Index predicate matches the WHERE in
-- MemoryRepo.ListAugmentationBackfillCandidates exactly. The planner only
-- chooses the partial index when the query's WHERE is a logical superset
-- of the predicate; an extra filter the predicate doesn't cover forces a
-- re-check on every row and (on SQLite) skips the index entirely.
DROP INDEX IF EXISTS idx_memories_augmented_backfill;
CREATE INDEX idx_memories_augmented_backfill
  ON memories (namespace_id, created_at)
  WHERE augmented_embedding_at IS NULL
    AND deleted_at IS NULL
    AND superseded_by IS NULL;
