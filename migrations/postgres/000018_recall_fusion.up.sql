-- Hybrid recall: replace the expression-based full-text index with a
-- generated tsvector column so the recall path can query content_tsv
-- directly (simpler EXPLAIN plans, no expression-match coupling). The
-- STORED column populates on ALTER TABLE; no explicit backfill needed.
ALTER TABLE memories
  ADD COLUMN content_tsv tsvector
  GENERATED ALWAYS AS (to_tsvector('english', content)) STORED;

CREATE INDEX idx_memories_content_tsv
  ON memories USING gin (content_tsv)
  WHERE deleted_at IS NULL;

DROP INDEX idx_memories_fulltext;
