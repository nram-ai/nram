-- Additive performance indexes. Each is purely a read-path speedup; none change
-- query results.

-- Enrichment claim hot path. ClaimNext runs continuously, selecting the
-- highest-priority oldest pending job: WHERE status='pending' ORDER BY
-- priority DESC, created_at ASC. The existing idx_enrichment_queue_status only
-- filters; the planner still sorts. A partial index on the ordering columns,
-- scoped to pending rows, serves both the filter and the sort and stays small.
CREATE INDEX IF NOT EXISTS idx_enrichment_queue_claim
  ON enrichment_queue (priority DESC, created_at ASC)
  WHERE status = 'pending';

-- Memory dedup / lineage live-row lookups. LookupByContentHash and
-- FindParentReplacements filter live (non-deleted) rows by namespace and the
-- superseded_by self-FK; this composite covers those probes without scanning
-- the namespace index and filtering superseded_by row by row.
CREATE INDEX IF NOT EXISTS idx_memories_ns_superseded
  ON memories (namespace_id, superseded_by)
  WHERE deleted_at IS NULL;

-- Dreaming list/retention queries order or range-scan by project then time.
-- The existing project-only indexes filter but force a separate sort; the
-- composite serves ListByProject (ORDER BY created_at DESC, via backward scan),
-- SelectSummaries, and the DeleteOlderThan retention range.
CREATE INDEX IF NOT EXISTS idx_dream_cycles_project_created
  ON dream_cycles (project_id, created_at);
CREATE INDEX IF NOT EXISTS idx_dream_log_summaries_project_created
  ON dream_log_summaries (project_id, created_at);
CREATE INDEX IF NOT EXISTS idx_dream_logs_project_created
  ON dream_logs (project_id, created_at);
