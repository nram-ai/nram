-- Add a heartbeat column the runner ticks during phase execution. The
-- existing updated_at only advances at phase boundaries, so a single
-- long phase looks indistinguishable from a crashed worker. heartbeat_at
-- is updated every dreaming.heartbeat_interval_seconds while a phase is
-- running, giving the admin UI a high-confidence "no recent activity"
-- signal independent of the conservative stuck-threshold used to gate
-- abandon.
ALTER TABLE dream_cycles ADD COLUMN heartbeat_at TIMESTAMPTZ;

-- Stuck-cycle sweeper queries `WHERE status='running' AND updated_at < ?`.
CREATE INDEX idx_dream_cycles_status_updated_at
  ON dream_cycles (status, updated_at);
