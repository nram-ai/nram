DROP INDEX IF EXISTS idx_memories_namespace_content_hash;
ALTER TABLE memories DROP COLUMN content_hash;
