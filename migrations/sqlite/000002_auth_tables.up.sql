-- Organizations table.
CREATE TABLE organizations (
  id            TEXT PRIMARY KEY,
  namespace_id  TEXT NOT NULL UNIQUE REFERENCES namespaces(id),
  name          TEXT NOT NULL,
  slug          TEXT NOT NULL UNIQUE,
  settings      TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Users table with RBAC roles.
-- Roles: administrator, org_owner, member, readonly, service
CREATE TABLE users (
  id            TEXT PRIMARY KEY,
  email         TEXT NOT NULL UNIQUE,
  display_name  TEXT DEFAULT '',
  password_hash TEXT,
  org_id        TEXT NOT NULL REFERENCES organizations(id),
  namespace_id  TEXT NOT NULL REFERENCES namespaces(id),
  role          TEXT NOT NULL DEFAULT 'member',
  settings      TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  last_login    TEXT,
  disabled_at   TEXT
);

CREATE INDEX idx_users_org ON users (org_id);

-- API keys table.
-- Keys are formatted as nram_k_ + 32 hex chars. Only SHA-256 hash is stored.
-- SQLite does not support array types; scopes stored as JSON array text.
CREATE TABLE api_keys (
  id            TEXT PRIMARY KEY,
  user_id       TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  key_prefix    TEXT NOT NULL,
  key_hash      TEXT NOT NULL UNIQUE,
  name          TEXT NOT NULL DEFAULT '',
  scopes        TEXT DEFAULT '[]',
  last_used     TEXT,
  expires_at    TEXT,
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
