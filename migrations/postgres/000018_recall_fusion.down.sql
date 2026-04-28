CREATE INDEX idx_memories_fulltext
  ON memories USING gin (to_tsvector('english', content));

DROP INDEX idx_memories_content_tsv;

ALTER TABLE memories DROP COLUMN content_tsv;
