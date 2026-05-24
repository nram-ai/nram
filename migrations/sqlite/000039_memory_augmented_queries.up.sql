-- Adds two columns recording embedding-augmentation state per memory:
--   augmented_queries        JSON array of paraphrase queries fed into the embedder
--   augmented_embedding_at   When the augmented embedding was last written
-- Both NULL means "embedded with raw content"; the partial index supports the
-- backfill query that finds memories needing augmentation.
ALTER TABLE memories ADD COLUMN augmented_queries TEXT;
ALTER TABLE memories ADD COLUMN augmented_embedding_at TEXT;

-- Index predicate matches the WHERE in
-- MemoryRepo.ListAugmentationBackfillCandidates exactly. Diverging the two
-- (e.g. dropping superseded_by IS NULL from the predicate while keeping it
-- in the query) leaves SQLite unable to recognize the partial index as
-- usable and forces a full-table scan on every backfill.
DROP INDEX IF EXISTS idx_memories_augmented_backfill;
CREATE INDEX idx_memories_augmented_backfill
  ON memories (namespace_id, created_at)
  WHERE augmented_embedding_at IS NULL
    AND deleted_at IS NULL
    AND superseded_by IS NULL;
