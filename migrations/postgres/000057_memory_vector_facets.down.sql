-- Reverse 000057: drop topic facets, restore the single-vector (memory_id)
-- primary key, and remove facet_id from the memory vector tables. Entity tables
-- were never touched.
DO $$
DECLARE
  dim text;
  tbl text;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') THEN
    FOREACH dim IN ARRAY ARRAY['384','512','768','1024','1536','3072'] LOOP
      tbl := 'memory_vectors_' || dim;
      EXECUTE format('DELETE FROM %I WHERE facet_id <> 0', tbl);
      EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', tbl, tbl || '_pkey');
      EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS facet_id', tbl);
      EXECUTE format('ALTER TABLE %I ADD PRIMARY KEY (memory_id)', tbl);
    END LOOP;
  END IF;
END $$;
