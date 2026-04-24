-- Add content_hash for ingest-time exact-content dedup. App-side computes
-- sha256(content) on Create; existing rows are populated by the startup
-- backfill loop. Index is non-unique because existing duplicates are tolerated
-- until a separate dedup tool merges them.
ALTER TABLE memories ADD COLUMN content_hash TEXT;
CREATE INDEX idx_memories_namespace_content_hash
  ON memories (namespace_id, content_hash)
  WHERE deleted_at IS NULL AND content_hash IS NOT NULL;
