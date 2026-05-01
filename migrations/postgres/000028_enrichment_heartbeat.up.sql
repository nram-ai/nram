-- Worker pool ticks heartbeat_at while a job is claimed. Without it, a
-- crashed worker leaves enrichment_queue rows stuck in status='processing'
-- forever; the existing updated_at only advances on claim/complete/fail,
-- so a long-running batch and a dead worker look identical from the row's
-- perspective. The StuckJobSweeper triggers on updated_at staleness
-- (safer); heartbeat_at is the tighter diagnostic column for the admin UI.
ALTER TABLE enrichment_queue ADD COLUMN heartbeat_at TIMESTAMPTZ;

-- Set by RequeueStale when the sweeper auto-requeues a stuck job, so the
-- admin queue row can render a "Requeued: ..." pill until the next worker
-- picks it up and Complete clears it.
ALTER TABLE enrichment_queue ADD COLUMN last_requeue_reason TEXT;

-- StuckJobSweeper queries `WHERE status='processing' AND updated_at < ?`.
CREATE INDEX idx_enrichment_queue_status_updated_at
  ON enrichment_queue (status, updated_at);
