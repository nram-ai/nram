-- SQLite cannot DROP a column with a foreign key reference cleanly via ALTER
-- TABLE before 3.35; we drop the indexes first and rely on the cascade-on-
-- table-drop to release the references. The down migration recreates the
-- oauth_* tables without the share_token_id column in the order that lets the
-- cascade clean up.
DROP INDEX IF EXISTS idx_oauth_refresh_tokens_share;
DROP INDEX IF EXISTS idx_oauth_clients_share;
DROP INDEX IF EXISTS idx_share_token_grants_project;
DROP INDEX IF EXISTS idx_share_tokens_expires;
DROP INDEX IF EXISTS idx_share_tokens_owner;

ALTER TABLE oauth_refresh_tokens DROP COLUMN share_token_id;
ALTER TABLE oauth_authorization_codes DROP COLUMN share_token_id;
ALTER TABLE oauth_clients DROP COLUMN share_token_id;

DROP TABLE IF EXISTS share_token_grants;
DROP TABLE IF EXISTS share_tokens;
