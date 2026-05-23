-- Partial b-tree index covering the pressure-prune hot path:
-- RelationshipRepo.ExpireLowestNTransitive (internal/storage/relationship_repo.go)
-- selects the N lowest-weight active transitive edges per namespace.
--
-- Predicate matches the query exactly so the planner can use a partial-index
-- scan; key order matches ORDER BY so LIMIT N terminates after reading N
-- entries. properties is jsonb; properties->>'source' is immutable for
-- index purposes.
--
-- DROP-then-CREATE (not CREATE IF NOT EXISTS) so any pre-existing
-- same-name index of a different shape from prototyping is replaced
-- with the reviewed shape rather than silently kept.
DROP INDEX IF EXISTS idx_relationships_transitive_prune;
CREATE INDEX idx_relationships_transitive_prune
  ON relationships (namespace_id, weight, created_at)
  WHERE valid_until IS NULL
    AND (properties->>'source') = 'transitive';
