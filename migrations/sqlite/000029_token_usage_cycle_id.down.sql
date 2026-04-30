DROP INDEX IF EXISTS idx_token_usage_cycle_id;

ALTER TABLE token_usage DROP COLUMN cycle_id;
