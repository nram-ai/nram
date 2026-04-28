DROP INDEX IF EXISTS idx_token_usage_success_time;
DROP INDEX IF EXISTS idx_token_usage_request_id;

ALTER TABLE token_usage DROP COLUMN request_id;
ALTER TABLE token_usage DROP COLUMN error_code;
ALTER TABLE token_usage DROP COLUMN success;
