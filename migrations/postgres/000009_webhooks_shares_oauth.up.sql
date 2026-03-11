-- Webhooks table (event subscriptions with delivery tracking).
CREATE TABLE webhooks (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  url           TEXT NOT NULL,
  secret        TEXT,
  events        TEXT[] NOT NULL,
  scope         TEXT NOT NULL DEFAULT 'global',
  active        BOOLEAN DEFAULT true,
  last_fired    TIMESTAMPTZ,
  last_status   INT,
  failure_count INT DEFAULT 0,
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Memory shares table (cross-namespace sharing permissions).
CREATE TABLE memory_shares (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_ns_id    UUID NOT NULL REFERENCES namespaces(id),
  target_ns_id    UUID NOT NULL REFERENCES namespaces(id),
  permission      TEXT NOT NULL DEFAULT 'recall',
  created_by      UUID REFERENCES users(id),
  expires_at      TIMESTAMPTZ,
  created_at      TIMESTAMPTZ DEFAULT now(),
  UNIQUE(source_ns_id, target_ns_id)
);

CREATE INDEX idx_shares_target ON memory_shares (target_ns_id);

-- Token usage table (LLM provider billing and analytics).
CREATE TABLE token_usage (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id        UUID REFERENCES organizations(id),
  user_id       UUID REFERENCES users(id),
  project_id    UUID REFERENCES projects(id),
  namespace_id  UUID NOT NULL REFERENCES namespaces(id),
  operation     TEXT NOT NULL,
  provider      TEXT NOT NULL,
  model         TEXT NOT NULL,
  tokens_input  INT NOT NULL DEFAULT 0,
  tokens_output INT NOT NULL DEFAULT 0,
  memory_id     UUID REFERENCES memories(id),
  api_key_id    UUID REFERENCES api_keys(id),
  latency_ms    INT,
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_token_usage_org_time ON token_usage (org_id, created_at);
CREATE INDEX idx_token_usage_user_time ON token_usage (user_id, created_at);
CREATE INDEX idx_token_usage_project_time ON token_usage (project_id, created_at);
CREATE INDEX idx_token_usage_operation ON token_usage (operation, created_at);

-- OAuth clients table (registered OAuth2 applications).
CREATE TABLE oauth_clients (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id       TEXT NOT NULL UNIQUE,
  client_secret   TEXT,
  name            TEXT NOT NULL,
  redirect_uris   TEXT[] NOT NULL,
  grant_types     TEXT[] DEFAULT '{"authorization_code","refresh_token"}',
  org_id          UUID REFERENCES organizations(id),
  auto_registered BOOLEAN DEFAULT false,
  created_at      TIMESTAMPTZ DEFAULT now()
);

-- OAuth authorization codes table (PKCE-capable auth code flow).
CREATE TABLE oauth_authorization_codes (
  code            TEXT PRIMARY KEY,
  client_id       TEXT NOT NULL REFERENCES oauth_clients(client_id),
  user_id         UUID NOT NULL REFERENCES users(id),
  redirect_uri    TEXT NOT NULL,
  scope           TEXT DEFAULT '',
  code_challenge  TEXT,
  code_challenge_method TEXT DEFAULT 'S256',
  expires_at      TIMESTAMPTZ NOT NULL,
  created_at      TIMESTAMPTZ DEFAULT now()
);

-- OAuth refresh tokens table (revocable long-lived tokens).
CREATE TABLE oauth_refresh_tokens (
  token_hash      TEXT PRIMARY KEY,
  client_id       TEXT NOT NULL REFERENCES oauth_clients(client_id),
  user_id         UUID NOT NULL REFERENCES users(id),
  scope           TEXT DEFAULT '',
  expires_at      TIMESTAMPTZ,
  revoked_at      TIMESTAMPTZ,
  created_at      TIMESTAMPTZ DEFAULT now()
);

-- OAuth identity provider configs table (external SSO integration).
CREATE TABLE oauth_idp_configs (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id          UUID REFERENCES organizations(id),
  provider_type   TEXT NOT NULL,
  client_id       TEXT NOT NULL,
  client_secret   TEXT NOT NULL,
  issuer_url      TEXT,
  allowed_domains TEXT[] DEFAULT '{}',
  auto_provision  BOOLEAN DEFAULT false,
  default_role    TEXT DEFAULT 'member',
  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now()
);
