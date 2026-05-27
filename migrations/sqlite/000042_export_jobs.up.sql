-- export_jobs: per-user asynchronous export queue. One row per request,
-- per-user scoped. project_id is NULL for account-wide exports; non-null
-- for single-project exports. The worker writes the artifact to
-- export.artifact_dir/<user_id>/<job_id>.zip (path captured in artifact_path)
-- and the cleanup sweep deletes rows where expires_at < now().
CREATE TABLE export_jobs (
  id                 TEXT PRIMARY KEY,
  user_id            TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  scope              TEXT NOT NULL,                       -- 'account' | 'project'
  project_id         TEXT REFERENCES projects(id) ON DELETE CASCADE,
  format             TEXT NOT NULL DEFAULT 'zip',         -- 'zip' (account) | 'json' | 'ndjson' (project, future)
  include_superseded INTEGER NOT NULL DEFAULT 0,
  status             TEXT NOT NULL DEFAULT 'pending',     -- 'pending' | 'processing' | 'succeeded' | 'failed' | 'expired'
  artifact_path      TEXT,
  artifact_bytes     INTEGER,
  artifact_sha256    TEXT,
  error              TEXT,
  claimed_by         TEXT,
  claimed_at         TEXT,
  started_at         TEXT,
  completed_at       TEXT,
  expires_at         TEXT,
  created_at         TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at         TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX idx_export_jobs_user_created ON export_jobs (user_id, created_at DESC);
CREATE INDEX idx_export_jobs_status_created ON export_jobs (status, created_at);
CREATE INDEX idx_export_jobs_expires ON export_jobs (expires_at) WHERE expires_at IS NOT NULL;
