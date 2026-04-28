-- Token usage observability: extend token_usage with success/error_code
-- so failed provider calls are distinguishable from successful ones in
-- analytics rollups, and request_id so multiple provider calls emitted
-- by one inbound request can be correlated.
ALTER TABLE token_usage ADD COLUMN success BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE token_usage ADD COLUMN error_code TEXT;
ALTER TABLE token_usage ADD COLUMN request_id TEXT;

CREATE INDEX IF NOT EXISTS idx_token_usage_request_id
  ON token_usage (request_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_success_time
  ON token_usage (success, created_at);
