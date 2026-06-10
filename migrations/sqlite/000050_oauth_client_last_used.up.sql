ALTER TABLE oauth_clients ADD COLUMN last_used_at TEXT;

-- Backfill from existing refresh-token history: each authorize and each refresh
-- writes an oauth_refresh_tokens row, so MAX(created_at) per client is the most
-- recent observed usage. created_at is RFC3339 and lexicographically sortable.
UPDATE oauth_clients
   SET last_used_at = (SELECT MAX(rt.created_at) FROM oauth_refresh_tokens rt
                       WHERE rt.client_id = oauth_clients.client_id);
