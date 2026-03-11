-- Organizations table.
CREATE TABLE organizations (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id  UUID NOT NULL UNIQUE REFERENCES namespaces(id),
  name          TEXT NOT NULL,
  slug          TEXT NOT NULL UNIQUE,
  settings      JSONB DEFAULT '{}',
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Users table with RBAC roles.
-- Roles: administrator, org_owner, member, readonly, service
CREATE TABLE users (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email         TEXT NOT NULL UNIQUE,
  display_name  TEXT DEFAULT '',
  password_hash TEXT,
  org_id        UUID NOT NULL REFERENCES organizations(id),
  namespace_id  UUID NOT NULL REFERENCES namespaces(id),
  role          TEXT NOT NULL DEFAULT 'member',
  settings      JSONB DEFAULT '{}',
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now(),
  last_login    TIMESTAMPTZ,
  disabled_at   TIMESTAMPTZ
);

CREATE INDEX idx_users_org ON users (org_id);

-- API keys table.
-- Keys are formatted as nram_k_ + 32 hex chars. Only SHA-256 hash is stored.
CREATE TABLE api_keys (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id       UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  key_prefix    TEXT NOT NULL,
  key_hash      TEXT NOT NULL UNIQUE,
  name          TEXT NOT NULL DEFAULT '',
  scopes        UUID[] DEFAULT '{}',
  last_used     TIMESTAMPTZ,
  expires_at    TIMESTAMPTZ,
  created_at    TIMESTAMPTZ DEFAULT now()
);
