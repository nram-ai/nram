-- Full-coverage (non-partial) endpoint indexes so the entity mention_count
-- recompute subquery can be planned as a BitmapOr of two index scans instead of
-- a per-entity sequential scan of the whole relationships table.
--
-- RecomputeMentionCounts / RecomputeMentionCountsByNamespace filter relationships
-- by (source_id = entity OR target_id = entity) with NO valid_until predicate (a
-- mention is counted across current and expired edges alike, as long as the
-- source memory is still live). The existing idx_relationships_source /
-- idx_relationships_target are PARTIAL (WHERE valid_until IS NULL), so Postgres
-- cannot use them for the filter-free subquery and falls back to a seq scan. On
-- the live corpus that makes the whole-table recompute a ~147M-cost UPDATE.
--
-- These twins carry every row, so the planner combines them via BitmapOr and the
-- recompute (scoped or full) drops to index cost. They are added ALONGSIDE the
-- partial pair, not replacing it, so active-edge queries keep their smaller
-- partial index and no existing query plan changes.
CREATE INDEX IF NOT EXISTS idx_relationships_source_all ON relationships (source_id);
CREATE INDEX IF NOT EXISTS idx_relationships_target_all ON relationships (target_id);
