-- Audit log of privileged actions. Backs the org-tier and system-tier
-- "activity" feeds in the admin dashboard, and supports per-actor and
-- per-target queries for forensic review.
--
-- Rows are append-only; deletion happens only through retention policy.
-- Foreign-key columns are nullable + unconstrained because the actor or
-- target may have been deleted by the time the audit row is read, and an
-- audit log is useless if it disappears with its referents.
CREATE TABLE audit_events (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor_user_id   UUID,
  actor_role      TEXT,
  action          TEXT NOT NULL,
  target_type     TEXT,
  target_id       UUID,
  target_org_id   UUID,
  source_ip       TEXT,
  user_agent      TEXT,
  details         JSONB DEFAULT '{}'
);

CREATE INDEX idx_audit_events_occurred_at
  ON audit_events (occurred_at DESC);
CREATE INDEX idx_audit_events_target_org_time
  ON audit_events (target_org_id, occurred_at DESC);
CREATE INDEX idx_audit_events_actor_time
  ON audit_events (actor_user_id, occurred_at DESC);
CREATE INDEX idx_audit_events_action_time
  ON audit_events (action, occurred_at DESC);
