-- Add ON DELETE CASCADE to FKs into entities(id). The original 000007 schema
-- gave entity_vectors_* this action but left entity_aliases.entity_id and
-- relationships.{source_id,target_id} with the default (NO ACTION). The
-- lifecycle orphan sweep, project-delete cascade, and any single-entity
-- delete all hit SQLSTATE 23503 the moment a child row exists. The
-- relationships FKs are not what fires for the orphan sweep itself (the
-- NOT IN filter excludes referenced entities), but adding CASCADE there too
-- removes the same footgun for every other delete path. Mirrors the FK-fix
-- shape used in 000020 for memories.
--
-- ADD CONSTRAINT ... NOT VALID + VALIDATE CONSTRAINT splits the work into a
-- short metadata-lock first phase and a non-blocking validation second phase.
-- Existing rows already satisfy these FKs (they did not change shape), so
-- VALIDATE is a formality that avoids leaving the constraint in NOT VALID
-- state for future planners.

ALTER TABLE entity_aliases
  DROP CONSTRAINT IF EXISTS entity_aliases_entity_id_fkey;
ALTER TABLE entity_aliases
  ADD CONSTRAINT entity_aliases_entity_id_fkey
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE entity_aliases VALIDATE CONSTRAINT entity_aliases_entity_id_fkey;

ALTER TABLE relationships
  DROP CONSTRAINT IF EXISTS relationships_source_id_fkey;
ALTER TABLE relationships
  ADD CONSTRAINT relationships_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES entities(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE relationships VALIDATE CONSTRAINT relationships_source_id_fkey;

ALTER TABLE relationships
  DROP CONSTRAINT IF EXISTS relationships_target_id_fkey;
ALTER TABLE relationships
  ADD CONSTRAINT relationships_target_id_fkey
    FOREIGN KEY (target_id) REFERENCES entities(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE relationships VALIDATE CONSTRAINT relationships_target_id_fkey;
