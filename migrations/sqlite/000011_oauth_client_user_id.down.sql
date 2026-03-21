DROP INDEX IF EXISTS idx_oauth_clients_user_id;
ALTER TABLE oauth_clients DROP COLUMN user_id;
