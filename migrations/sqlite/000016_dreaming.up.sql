-- Dream cycles track each complete dream processing run for a project.
CREATE TABLE dream_cycles (
  id             TEXT PRIMARY KEY,
  project_id     TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  namespace_id   TEXT NOT NULL REFERENCES namespaces(id),
  status         TEXT NOT NULL DEFAULT 'pending',
  phase          TEXT DEFAULT '',
  tokens_used    INTEGER DEFAULT 0,
  token_budget   INTEGER DEFAULT 0,
  phase_summary  TEXT DEFAULT '{}',
  error          TEXT,
  started_at     TEXT,
  completed_at   TEXT,
  created_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX idx_dream_cycles_project ON dream_cycles (project_id);
CREATE INDEX idx_dream_cycles_status ON dream_cycles (status);
CREATE INDEX idx_dream_cycles_namespace ON dream_cycles (namespace_id);

-- Dream log entries store before/after snapshots for each operation in a cycle.
CREATE TABLE dream_logs (
  id            TEXT PRIMARY KEY,
  cycle_id      TEXT NOT NULL REFERENCES dream_cycles(id) ON DELETE CASCADE,
  project_id    TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  phase         TEXT NOT NULL,
  operation     TEXT NOT NULL,
  target_type   TEXT NOT NULL,
  target_id     TEXT NOT NULL,
  before_state  TEXT DEFAULT '{}',
  after_state   TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX idx_dream_logs_cycle ON dream_logs (cycle_id);
CREATE INDEX idx_dream_logs_project ON dream_logs (project_id);
CREATE INDEX idx_dream_logs_target ON dream_logs (target_type, target_id);

-- Dream log summaries replace detailed logs after the retention window.
CREATE TABLE dream_log_summaries (
  id          TEXT PRIMARY KEY,
  cycle_id    TEXT NOT NULL REFERENCES dream_cycles(id) ON DELETE CASCADE,
  project_id  TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  summary     TEXT DEFAULT '{}',
  created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX idx_dream_log_summaries_project ON dream_log_summaries (project_id);

-- Tracks which projects have unprocessed user-originated changes.
CREATE TABLE dream_project_dirty (
  project_id    TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
  dirty_since   TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  last_dream_at TEXT
);
