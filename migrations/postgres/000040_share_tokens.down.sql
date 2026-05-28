DROP INDEX IF EXISTS idx_oauth_refresh_tokens_share;
DROP INDEX IF EXISTS idx_oauth_clients_share;
DROP INDEX IF EXISTS idx_share_token_grants_project;
DROP INDEX IF EXISTS idx_share_tokens_expires;
DROP INDEX IF EXISTS idx_share_tokens_owner;

ALTER TABLE oauth_refresh_tokens DROP COLUMN IF EXISTS share_token_id;
ALTER TABLE oauth_authorization_codes DROP COLUMN IF EXISTS share_token_id;
ALTER TABLE oauth_clients DROP COLUMN IF EXISTS share_token_id;

DROP TABLE IF EXISTS share_token_grants;
DROP TABLE IF EXISTS share_tokens;
