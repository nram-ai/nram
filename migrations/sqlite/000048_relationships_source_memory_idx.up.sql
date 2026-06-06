-- Index relationships by their source memory. The about_me persona "framing"
-- ordering joins memories -> relationships(source_memory) -> entities to rank a
-- memory by the mention_count of the entities it links to; without this index
-- that join scans the relationships table per memory.
CREATE INDEX IF NOT EXISTS idx_relationships_source_memory ON relationships (source_memory);
