-- Revert FKs into entities(id) to the default (NO ACTION). Restores the
-- pre-000032 schema where deleting an entity with surviving aliases or
-- relationships fails with SQLSTATE 23503.

ALTER TABLE entity_aliases
  DROP CONSTRAINT IF EXISTS entity_aliases_entity_id_fkey;
ALTER TABLE entity_aliases
  ADD CONSTRAINT entity_aliases_entity_id_fkey
    FOREIGN KEY (entity_id) REFERENCES entities(id);

ALTER TABLE relationships
  DROP CONSTRAINT IF EXISTS relationships_source_id_fkey;
ALTER TABLE relationships
  ADD CONSTRAINT relationships_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES entities(id);

ALTER TABLE relationships
  DROP CONSTRAINT IF EXISTS relationships_target_id_fkey;
ALTER TABLE relationships
  ADD CONSTRAINT relationships_target_id_fkey
    FOREIGN KEY (target_id) REFERENCES entities(id);
