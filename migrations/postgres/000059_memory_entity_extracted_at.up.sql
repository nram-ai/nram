-- Records when entity extraction last ran for a memory. NULL means "entities
-- never extracted". The consolidation-entity backfill candidate query
-- (MemoryRepo.ListDreamEntityBackfillCandidates) gates on
-- entity_extracted_at IS NULL so a consolidation dream that extracts entities
-- but no relationships still drops out of the candidate set once extraction has
-- run, instead of re-extracting every cycle (the relationship-presence gate
-- alone never converges for an entity-only synthesis). Stamped at enrichment
-- finalize whenever entity extraction was performed; omitted from INSERT so it
-- defaults NULL at creation.
ALTER TABLE memories ADD COLUMN entity_extracted_at TIMESTAMPTZ;
