ALTER TABLE oauth_clients ADD COLUMN user_id TEXT REFERENCES users(id);
CREATE INDEX idx_oauth_clients_user_id ON oauth_clients (user_id);
