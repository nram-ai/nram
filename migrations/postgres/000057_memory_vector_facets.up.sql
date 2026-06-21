-- Add facet_id to the memory vector tables so a memory can carry multiple
-- vectors: facet 0 is the pooled whole-memory embedding (every existing row
-- becomes facet 0 via the default), and facets 1..N are topic facets produced
-- by multi-vector extraction. The primary key becomes (memory_id, facet_id).
-- Entity vector tables are intentionally left single-vector: entities are
-- single-concept. Guarded on the pgvector extension, mirroring 000006.
DO $$
DECLARE
  dim text;
  tbl text;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') THEN
    FOREACH dim IN ARRAY ARRAY['384','512','768','1024','1536','3072'] LOOP
      tbl := 'memory_vectors_' || dim;
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS facet_id smallint NOT NULL DEFAULT 0', tbl);
      EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', tbl, tbl || '_pkey');
      EXECUTE format('ALTER TABLE %I ADD PRIMARY KEY (memory_id, facet_id)', tbl);
    END LOOP;
  END IF;
END $$;
