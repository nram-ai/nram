-- log_entries stores diagnostic log records captured from the application's
-- structured logger (slog) for the system operator's Logs view. It is
-- system-global and admin-only: a record is not owned by a tenant, so
-- project_id / namespace_id / user_id are nullable and carry no foreign keys.
-- They are populated only when the originating log carried them as attributes,
-- and logs deliberately outlive the rows they reference (no cascade delete).
-- The rolling window is enforced by the logging retention sweeper, not the DB.
-- Timestamps are stored as RFC3339 UTC strings (fixed-width, Z-suffixed) so a
-- lexical ORDER BY ts is chronological, matching the dream_logs convention.
CREATE TABLE log_entries (
  id            TEXT PRIMARY KEY,
  ts            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  level         TEXT NOT NULL,
  component     TEXT,
  message       TEXT NOT NULL,
  attrs         TEXT NOT NULL DEFAULT '{}',
  project_id    TEXT,
  namespace_id  TEXT,
  user_id       TEXT
);

CREATE INDEX idx_log_entries_ts ON log_entries (ts DESC);
CREATE INDEX idx_log_entries_level_ts ON log_entries (level, ts DESC);
CREATE INDEX idx_log_entries_component_ts ON log_entries (component, ts DESC);
