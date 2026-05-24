DROP INDEX IF EXISTS idx_memories_augmented_backfill;
ALTER TABLE memories DROP COLUMN augmented_embedding_at;
ALTER TABLE memories DROP COLUMN augmented_queries;
