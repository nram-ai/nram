-- Share tokens: capability-bearer credentials for granting external recipients
-- scoped read/write access to a curated set of an owner's projects without the
-- recipient having an nram account. The token itself is the identity; grants
-- are looked up from share_token_grants on every request so edits take effect
-- immediately. nram_s_<secret> is the wire format; only the SHA-256 hash is
-- stored. token_prefix is the first 8 chars after the prefix, surfaced in the
-- admin UI so the owner can identify a share without seeing the secret again.
CREATE TABLE share_tokens (
  id            TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash    TEXT NOT NULL UNIQUE,
  token_prefix  TEXT NOT NULL,
  name          TEXT NOT NULL,
  description   TEXT,
  is_one_shot   INTEGER NOT NULL DEFAULT 0,
  expires_at    TEXT NOT NULL,
  consumed_at   TEXT,
  created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  last_used_at  TEXT,
  use_count     INTEGER NOT NULL DEFAULT 0,
  revoked_at    TEXT
);

CREATE INDEX idx_share_tokens_owner ON share_tokens (owner_user_id, created_at DESC);
CREATE INDEX idx_share_tokens_expires ON share_tokens (expires_at) WHERE revoked_at IS NULL;

-- Per-project grants attached to a share. permission is one of:
--   'read'              → recall, list, get, graph, list_projects
--   'read_store'        → read + store, store_batch
--   'read_store_modify' → read_store + update, forget
-- delete_project and update_project are never granted to share-bearers; the
-- owner retains those exclusively.
CREATE TABLE share_token_grants (
  share_token_id TEXT NOT NULL REFERENCES share_tokens(id) ON DELETE CASCADE,
  project_id     TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  permission     TEXT NOT NULL CHECK (permission IN ('read', 'read_store', 'read_store_modify')),
  PRIMARY KEY (share_token_id, project_id)
);

CREATE INDEX idx_share_token_grants_project ON share_token_grants (project_id);

-- Bind OAuth artifacts back to the share that produced them so cascade-on-
-- revoke kills every derived credential. Nullable because the existing
-- account-holder OAuth flow does not involve a share.
ALTER TABLE oauth_clients ADD COLUMN share_token_id TEXT REFERENCES share_tokens(id) ON DELETE CASCADE;
ALTER TABLE oauth_authorization_codes ADD COLUMN share_token_id TEXT REFERENCES share_tokens(id) ON DELETE CASCADE;
ALTER TABLE oauth_refresh_tokens ADD COLUMN share_token_id TEXT REFERENCES share_tokens(id) ON DELETE CASCADE;

CREATE INDEX idx_oauth_clients_share ON oauth_clients (share_token_id) WHERE share_token_id IS NOT NULL;
CREATE INDEX idx_oauth_refresh_tokens_share ON oauth_refresh_tokens (share_token_id) WHERE share_token_id IS NOT NULL;
