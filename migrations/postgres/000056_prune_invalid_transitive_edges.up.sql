-- One-time cleanup of invalid inferred (transitive) relationships.
--
-- The transitive discovery phase previously copied the FIRST hop's relation
-- label onto an unrelated second hop and treated every relation as transitive,
-- producing edges such as "Emma Lehmann --wife of--> sglang". The phase now only
-- infers A->C when both hops carry the SAME relation AND that relation is in the
-- allowed transitive set. This migration deletes every existing transitive edge
-- that the corrected rule would not have created, keeping the valid remainder.
--
-- An active transitive edge is VALID iff: source != target, its relation is in
-- the allowed set (canonical forms, frozen here to match the shipped default of
-- dreaming.transitive.relations), and a same-relation two-hop path of direct
-- (non-inferred) edges A->B->C backs it. Everything else is deleted.
--
-- Idempotent: re-running deletes nothing once the invalid edges are gone.
-- Irreversible: deleted derived edges cannot be reconstructed (the .down is a
-- no-op); the corrected phase re-derives valid ones on later dream cycles.
DELETE FROM relationships
WHERE id IN (
  SELECT r.id
  FROM relationships r
  WHERE r.valid_until IS NULL
    AND r.properties->>'source' = 'transitive'
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
         AND (e2.properties->>'source') IS DISTINCT FROM 'transitive'
        WHERE e1.namespace_id = r.namespace_id
          AND e1.valid_until IS NULL
          AND (e1.properties->>'source') IS DISTINCT FROM 'transitive'
          AND e1.source_id = r.source_id
          AND e2.target_id = r.target_id
          AND e1.relation = r.relation
      )
    )
);
