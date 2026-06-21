DROP INDEX IF EXISTS idx_memories_facet_backfill;
ALTER TABLE memories DROP COLUMN facet_count;
ALTER TABLE memories DROP COLUMN faceted_at;
