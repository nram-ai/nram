DROP INDEX IF EXISTS idx_dream_cycles_status_updated_at;
ALTER TABLE dream_cycles DROP COLUMN heartbeat_at;
