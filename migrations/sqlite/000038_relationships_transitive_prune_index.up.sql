-- Partial expression index covering the pressure-prune hot path:
-- RelationshipRepo.ExpireLowestNTransitive (internal/storage/relationship_repo.go)
-- selects the N lowest-weight active transitive edges per namespace.
--
-- Predicate matches the query exactly so SQLite can use a partial-index scan;
-- key order matches ORDER BY so LIMIT N terminates after reading N entries.
-- json_extract is deterministic and legal in both expression keys and the
-- partial WHERE clause.
--
-- DROP-then-CREATE (not CREATE IF NOT EXISTS) so any pre-existing
-- same-name index of a different shape from prototyping is replaced
-- with the reviewed shape rather than silently kept.
DROP INDEX IF EXISTS idx_relationships_transitive_prune;
CREATE INDEX idx_relationships_transitive_prune
  ON relationships (namespace_id, weight, created_at)
  WHERE valid_until IS NULL
    AND json_extract(properties, '$.source') = 'transitive';
