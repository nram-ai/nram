-- One-time cleanup of invalid inferred (transitive) relationships. SQLite
-- counterpart of the postgres 000056 migration; see that file for the full
-- rationale. Differs only in JSON access (json_extract) and null-safe
-- comparison (IS NOT vs IS DISTINCT FROM).
--
-- An active transitive edge is VALID iff: source != target, its relation is in
-- the allowed set (canonical forms, frozen to match the shipped default of
-- dreaming.transitive.relations), and a same-relation two-hop path of direct
-- (non-inferred) edges A->B->C backs it. Everything else is deleted.
--
-- Idempotent: re-running deletes nothing once the invalid edges are gone.
-- Irreversible: the .down migration is a no-op.
DELETE FROM relationships
WHERE id IN (
  SELECT r.id
  FROM relationships r
  WHERE r.valid_until IS NULL
    AND json_extract(r.properties, '$.source') = 'transitive'
    AND NOT (
      r.source_id <> r.target_id
      AND r.relation IN (
        'part of','is part of','contains','located in','is located in',
        'depends on','subclass of','is a','type of','ancestor of',
        'descendant of','broader than','narrower than'
      )
      AND EXISTS (
        SELECT 1
        FROM relationships e1
        JOIN relationships e2
          ON e2.source_id = e1.target_id
         AND e2.relation = e1.relation
         AND e2.namespace_id = e1.namespace_id
         AND e2.valid_until IS NULL
         AND json_extract(e2.properties, '$.source') IS NOT 'transitive'
        WHERE e1.namespace_id = r.namespace_id
          AND e1.valid_until IS NULL
          AND json_extract(e1.properties, '$.source') IS NOT 'transitive'
          AND e1.source_id = r.source_id
          AND e2.target_id = r.target_id
          AND e1.relation = r.relation
      )
    )
);
