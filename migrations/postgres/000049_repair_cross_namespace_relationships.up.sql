-- Security repair: delete relationships whose namespace_id does not match the
-- namespace of one (or both) of their endpoint entities. Such an edge is
-- invalid by construction (an entity belongs to exactly one namespace, so a
-- well-formed edge lives in its endpoints' shared namespace) and is what let
-- the graph traversal leak rows across projects/tenants before the read
-- primitives were namespace-bounded. Endpoints that no longer exist (dangling
-- edges) are left to the existing dangling-relationship sweep; this migration
-- only purges the cross-namespace rows. Idempotent: a no-op on a clean store.
DELETE FROM relationships r
USING entities se, entities te
WHERE se.id = r.source_id
  AND te.id = r.target_id
  AND (r.namespace_id <> se.namespace_id OR r.namespace_id <> te.namespace_id);
