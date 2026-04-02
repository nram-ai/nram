-- Dream cycles track each complete dream processing run for a project.
CREATE TABLE dream_cycles (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id     UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  namespace_id   UUID NOT NULL REFERENCES namespaces(id),
  status         TEXT NOT NULL DEFAULT 'pending',
  phase          TEXT DEFAULT '',
  tokens_used    INT DEFAULT 0,
  token_budget   INT DEFAULT 0,
  phase_summary  JSONB DEFAULT '{}',
  error          TEXT,
  started_at     TIMESTAMPTZ,
  completed_at   TIMESTAMPTZ,
  created_at     TIMESTAMPTZ DEFAULT now(),
  updated_at     TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_dream_cycles_project ON dream_cycles (project_id);
CREATE INDEX idx_dream_cycles_status ON dream_cycles (status);
CREATE INDEX idx_dream_cycles_namespace ON dream_cycles (namespace_id);
CREATE INDEX idx_dream_cycles_project_completed
  ON dream_cycles (project_id, completed_at DESC) WHERE status = 'completed';

-- Dream log entries store before/after snapshots for each operation in a cycle.
CREATE TABLE dream_logs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cycle_id      UUID NOT NULL REFERENCES dream_cycles(id) ON DELETE CASCADE,
  project_id    UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  phase         TEXT NOT NULL,
  operation     TEXT NOT NULL,
  target_type   TEXT NOT NULL,
  target_id     UUID NOT NULL,
  before_state  JSONB DEFAULT '{}',
  after_state   JSONB DEFAULT '{}',
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_dream_logs_cycle ON dream_logs (cycle_id);
CREATE INDEX idx_dream_logs_project ON dream_logs (project_id);
CREATE INDEX idx_dream_logs_target ON dream_logs (target_type, target_id);

-- Dream log summaries replace detailed logs after the retention window.
CREATE TABLE dream_log_summaries (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cycle_id    UUID NOT NULL REFERENCES dream_cycles(id) ON DELETE CASCADE,
  project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  summary     JSONB DEFAULT '{}',
  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_dream_log_summaries_project ON dream_log_summaries (project_id);

-- Tracks which projects have unprocessed user-originated changes.
CREATE TABLE dream_project_dirty (
  project_id    UUID PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
  dirty_since   TIMESTAMPTZ DEFAULT now(),
  last_dream_at TIMESTAMPTZ
);
