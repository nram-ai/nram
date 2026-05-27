-- export_jobs: per-user asynchronous export queue. One row per request,
-- per-user scoped. project_id is NULL for account-wide exports; non-null
-- for single-project exports. The worker writes the artifact to
-- export.artifact_dir/<user_id>/<job_id>.zip (path captured in artifact_path)
-- and the cleanup sweep deletes rows where expires_at < now().
CREATE TABLE export_jobs (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id            UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  scope              TEXT NOT NULL,
  project_id         UUID REFERENCES projects(id) ON DELETE CASCADE,
  format             TEXT NOT NULL DEFAULT 'zip',
  include_superseded BOOLEAN NOT NULL DEFAULT false,
  status             TEXT NOT NULL DEFAULT 'pending',
  artifact_path      TEXT,
  artifact_bytes     BIGINT,
  artifact_sha256    TEXT,
  error              TEXT,
  claimed_by         TEXT,
  claimed_at         TIMESTAMPTZ,
  started_at         TIMESTAMPTZ,
  completed_at       TIMESTAMPTZ,
  expires_at         TIMESTAMPTZ,
  created_at         TIMESTAMPTZ DEFAULT now(),
  updated_at         TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_export_jobs_user_created ON export_jobs (user_id, created_at DESC);
CREATE INDEX idx_export_jobs_status_created ON export_jobs (status, created_at);
CREATE INDEX idx_export_jobs_expires ON export_jobs (expires_at) WHERE expires_at IS NOT NULL;
