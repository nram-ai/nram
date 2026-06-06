-- Adds the typed `origin` column: the coarse, server-assigned provenance
-- category (user | dream | import). This replaces the historical practice of
-- overloading the free-form `source` string with the literal "dream" as a
-- control signal. `origin` is the authoritative discriminator internal logic
-- branches on; `source` reverts to a pure free-form label.
ALTER TABLE memories ADD COLUMN origin TEXT NOT NULL DEFAULT 'user';

-- Backfill from existing provenance signals.
--   dream  : every memory the consolidation cycle wrote carried source='dream'.
--   import : the mem0 and zep importers stamp memory.source='mem0-import' /
--            'zep-import' (internal/service/import.go). NOTE: the nram importer
--            preserves each item's ORIGINAL source on the memory row (the
--            '<format>-import' label only lands in ingestion_log.source), so
--            historical nram imports carry no row-level marker and cannot be
--            reclassified here: they remain 'user'. New imports of every format
--            are tagged Origin=OriginImport at write time.
UPDATE memories SET origin = 'dream'  WHERE source = 'dream';
UPDATE memories SET origin = 'import' WHERE source IN ('mem0-import', 'zep-import');

-- Retire the "dream" string from the source column entirely. With origin now
-- authoritative, the string is reserved and must never reappear, so the legacy
-- rows that carried it are cleared. Dream provenance lives in origin from here.
UPDATE memories SET source = NULL WHERE source = 'dream';
