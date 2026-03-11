-- Webhooks table (event subscriptions with delivery tracking).
CREATE TABLE webhooks (
  id            TEXT PRIMARY KEY,
  url           TEXT NOT NULL,
  secret        TEXT,
  events        TEXT NOT NULL,
  scope         TEXT NOT NULL DEFAULT 'global',
  active        INTEGER DEFAULT 1,
  last_fired    TEXT,
  last_status   INTEGER,
  failure_count INTEGER DEFAULT 0,
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Memory shares table (cross-namespace sharing permissions).
CREATE TABLE memory_shares (
  id              TEXT PRIMARY KEY,
  source_ns_id    TEXT NOT NULL REFERENCES namespaces(id),
  target_ns_id    TEXT NOT NULL REFERENCES namespaces(id),
  permission      TEXT NOT NULL DEFAULT 'recall',
  created_by      TEXT REFERENCES users(id),
  expires_at      TEXT,
  revoked_at      TEXT,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(source_ns_id, target_ns_id)
);

CREATE INDEX idx_shares_target ON memory_shares (target_ns_id);

-- OAuth clients table (registered OAuth2 applications).
CREATE TABLE oauth_clients (
  id              TEXT PRIMARY KEY,
  client_id       TEXT NOT NULL UNIQUE,
  client_secret   TEXT,
  name            TEXT NOT NULL,
  redirect_uris   TEXT NOT NULL,
  grant_types     TEXT DEFAULT '["authorization_code","refresh_token"]',
  org_id          TEXT REFERENCES organizations(id),
  auto_registered INTEGER DEFAULT 0,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- OAuth authorization codes table (PKCE-capable auth code flow).
CREATE TABLE oauth_authorization_codes (
  code            TEXT PRIMARY KEY,
  client_id       TEXT NOT NULL REFERENCES oauth_clients(client_id),
  user_id         TEXT NOT NULL REFERENCES users(id),
  redirect_uri    TEXT NOT NULL,
  scope           TEXT DEFAULT '',
  code_challenge  TEXT,
  code_challenge_method TEXT DEFAULT 'S256',
  expires_at      TEXT NOT NULL,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- OAuth refresh tokens table (revocable long-lived tokens).
CREATE TABLE oauth_refresh_tokens (
  token_hash      TEXT PRIMARY KEY,
  client_id       TEXT NOT NULL REFERENCES oauth_clients(client_id),
  user_id         TEXT NOT NULL REFERENCES users(id),
  scope           TEXT DEFAULT '',
  expires_at      TEXT,
  revoked_at      TEXT,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- OAuth identity provider configs table (external SSO integration).
CREATE TABLE oauth_idp_configs (
  id              TEXT PRIMARY KEY,
  org_id          TEXT REFERENCES organizations(id),
  provider_type   TEXT NOT NULL,
  client_id       TEXT NOT NULL,
  client_secret   TEXT NOT NULL,
  issuer_url      TEXT,
  allowed_domains TEXT DEFAULT '[]',
  auto_provision  INTEGER DEFAULT 0,
  default_role    TEXT DEFAULT 'member',
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
