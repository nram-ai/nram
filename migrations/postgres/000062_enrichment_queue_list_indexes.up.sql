-- Additive read-path indexes for the enrichment queue view. None change query
-- results. They make the "My Enrichment Queue" page's counts, list, and the
-- sweeper's failed-row prunes index-served instead of full-scanning a queue that
-- has accumulated a large failed backlog.

-- Scoped counts + scoped list. SelfQueueStatus / OrgQueueStatus count by status
-- within a namespace subtree (namespace_id IN (subtree)) and list a single
-- status ordered by created_at. This composite serves the GROUP BY count, the
-- status filter, and the created_at ordering from one index. It also serves the
-- per-namespace failed-row count cap (ROW_NUMBER PARTITION BY namespace_id
-- ORDER BY created_at).
CREATE INDEX IF NOT EXISTS idx_enrichment_queue_ns_status_created
  ON enrichment_queue (namespace_id, status, created_at);

-- System-wide list filtered by a single status and ordered by created_at (the
-- QueueStatus items query with a status filter). The single-column
-- idx_enrichment_queue_status only filters; the planner still sorts without this.
-- (The age-retention prune ranges on updated_at, served by
-- idx_enrichment_queue_status_updated_at, not this index.)
CREATE INDEX IF NOT EXISTS idx_enrichment_queue_status_created
  ON enrichment_queue (status, created_at);

-- Extraction-health tally (system tier) sums LIKE matches over rows that carry a
-- last_error, bounded to a recent created_at window. This partial index covers
-- only error rows and serves that bounded scan.
CREATE INDEX IF NOT EXISTS idx_enrichment_queue_error_created
  ON enrichment_queue (created_at)
  WHERE last_error IS NOT NULL;
