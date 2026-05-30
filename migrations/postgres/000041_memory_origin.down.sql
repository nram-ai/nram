-- Drops the origin column. The source='dream' clearing in the up-migration is
-- intentionally NOT reversed: the "dream" string is being retired and its
-- provenance is recoverable from the memory_lineage table (synthesized_from).
ALTER TABLE memories DROP COLUMN origin;
