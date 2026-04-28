-- Mirrors postgres 000020 (per-FK rationale lives there). SQLite cannot
-- ALTER a FK action in place, so each table is rebuilt via the documented
-- pattern: create _new with the new constraint, copy data, drop, rename,
-- re-create indexes. defer_foreign_keys = 1 lets the rebuild finish inside
-- golang-migrate's transaction without intermediate FK errors.
--
-- The memories.superseded_by self-FK is NOT rebuilt here: that would force
-- recreating the memories table along with its FTS5 triggers and indexes,
-- and the supersession-chain hard-delete case is rare enough not to warrant
-- it. The retention sweep failure that drove this migration is on
-- token_usage; that's covered.

PRAGMA defer_foreign_keys = 1;

-- token_usage rebuild: memory_id ON DELETE SET NULL.
CREATE TABLE token_usage_new (
  id            TEXT PRIMARY KEY,
  org_id        TEXT REFERENCES organizations(id),
  user_id       TEXT REFERENCES users(id),
  project_id    TEXT REFERENCES projects(id),
  namespace_id  TEXT NOT NULL REFERENCES namespaces(id),
  operation     TEXT NOT NULL,
  provider      TEXT NOT NULL,
  model         TEXT NOT NULL,
  tokens_input  INTEGER NOT NULL DEFAULT 0,
  tokens_output INTEGER NOT NULL DEFAULT 0,
  memory_id     TEXT REFERENCES memories(id) ON DELETE SET NULL,
  api_key_id    TEXT REFERENCES api_keys(id),
  latency_ms    INTEGER,
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  success       INTEGER NOT NULL DEFAULT 1,
  error_code    TEXT,
  request_id    TEXT
);

INSERT INTO token_usage_new (
  id, org_id, user_id, project_id, namespace_id, operation, provider, model,
  tokens_input, tokens_output, memory_id, api_key_id, latency_ms, created_at,
  success, error_code, request_id
)
SELECT
  id, org_id, user_id, project_id, namespace_id, operation, provider, model,
  tokens_input, tokens_output, memory_id, api_key_id, latency_ms, created_at,
  success, error_code, request_id
FROM token_usage;

DROP TABLE token_usage;
ALTER TABLE token_usage_new RENAME TO token_usage;

CREATE INDEX idx_token_usage_org_time ON token_usage (org_id, created_at);
CREATE INDEX idx_token_usage_user_time ON token_usage (user_id, created_at);
CREATE INDEX idx_token_usage_project_time ON token_usage (project_id, created_at);
CREATE INDEX idx_token_usage_operation ON token_usage (operation, created_at);
CREATE INDEX idx_token_usage_request_id ON token_usage (request_id);
CREATE INDEX idx_token_usage_success_time ON token_usage (success, created_at);

-- memory_lineage rebuild: memory_id CASCADE; parent_id SET NULL.
CREATE TABLE memory_lineage_new (
  id            TEXT PRIMARY KEY,
  memory_id     TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  parent_id     TEXT REFERENCES memories(id) ON DELETE SET NULL,
  relation      TEXT NOT NULL,
  context       TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  namespace_id  TEXT REFERENCES namespaces(id)
);

INSERT INTO memory_lineage_new (
  id, memory_id, parent_id, relation, context, created_at, namespace_id
)
SELECT
  id, memory_id, parent_id, relation, context, created_at, namespace_id
FROM memory_lineage;

DROP TABLE memory_lineage;
ALTER TABLE memory_lineage_new RENAME TO memory_lineage;

CREATE INDEX idx_memory_lineage_namespace ON memory_lineage (namespace_id);

-- enrichment_queue rebuild: memory_id CASCADE.
CREATE TABLE enrichment_queue_new (
  id              TEXT PRIMARY KEY,
  memory_id       TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  namespace_id    TEXT NOT NULL REFERENCES namespaces(id),
  status          TEXT NOT NULL DEFAULT 'pending',
  priority        INTEGER DEFAULT 0,
  claimed_at      TEXT,
  claimed_by      TEXT,
  attempts        INTEGER DEFAULT 0,
  max_attempts    INTEGER DEFAULT 3,
  last_error      TEXT,
  steps_completed TEXT DEFAULT '[]',
  completed_at    TEXT,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT INTO enrichment_queue_new (
  id, memory_id, namespace_id, status, priority, claimed_at, claimed_by,
  attempts, max_attempts, last_error, steps_completed, completed_at,
  created_at, updated_at
)
SELECT
  id, memory_id, namespace_id, status, priority, claimed_at, claimed_by,
  attempts, max_attempts, last_error, steps_completed, completed_at,
  created_at, updated_at
FROM enrichment_queue;

DROP TABLE enrichment_queue;
ALTER TABLE enrichment_queue_new RENAME TO enrichment_queue;

CREATE INDEX idx_enrichment_queue_namespace ON enrichment_queue (namespace_id);
CREATE INDEX idx_enrichment_queue_memory ON enrichment_queue (memory_id);
CREATE INDEX idx_enrichment_queue_status ON enrichment_queue (status);

-- relationships rebuild: source_memory SET NULL.
CREATE TABLE relationships_new (
  id              TEXT PRIMARY KEY,
  namespace_id    TEXT NOT NULL REFERENCES namespaces(id),
  source_id       TEXT NOT NULL REFERENCES entities(id),
  target_id       TEXT NOT NULL REFERENCES entities(id),
  relation        TEXT NOT NULL,
  weight          REAL DEFAULT 1.0,
  properties      TEXT DEFAULT '{}',
  valid_from      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  valid_until     TEXT,
  source_memory   TEXT REFERENCES memories(id) ON DELETE SET NULL,
  created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(namespace_id, source_id, target_id, relation, valid_from)
);

INSERT INTO relationships_new (
  id, namespace_id, source_id, target_id, relation, weight, properties,
  valid_from, valid_until, source_memory, created_at
)
SELECT
  id, namespace_id, source_id, target_id, relation, weight, properties,
  valid_from, valid_until, source_memory, created_at
FROM relationships;

DROP TABLE relationships;
ALTER TABLE relationships_new RENAME TO relationships;

CREATE INDEX idx_relationships_source ON relationships (source_id);
CREATE INDEX idx_relationships_target ON relationships (target_id);
CREATE INDEX idx_relationships_namespace ON relationships (namespace_id);
