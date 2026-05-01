DROP INDEX IF EXISTS idx_enrichment_queue_status_updated_at;
ALTER TABLE enrichment_queue DROP COLUMN last_requeue_reason;
ALTER TABLE enrichment_queue DROP COLUMN heartbeat_at;
