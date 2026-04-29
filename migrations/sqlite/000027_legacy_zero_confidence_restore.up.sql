-- Restore confidence=1.0 for memory rows that were silently filtered from
-- recall by the kill-signal at internal/service/recall.go:710. Confidence=0
-- has three legitimate origins:
--
--   1. Legacy rows from before the column was actively populated (these are
--      the ones we want to restore).
--   2. Dream alignment scoring driving newConfidence below zero, clamped at
--      0 by phase_consolidation.go (a deliberate "demote" signal that we
--      MUST preserve).
--   3. Future explicit operator zeroing (does not exist yet at deploy time).
--
-- The dream_logs operation `confidence_adjusted` is the canonical evidence
-- of (2). Restore only memories whose ID does NOT appear in any
-- `confidence_adjusted` log entry for target_type='memory'.
--
-- Caveat: dream_logs are summarized after dreaming.log_retention_days
-- (default 30). For deployments where reinforcement has been live longer
-- than that retention window, some demoted rows may be indistinguishable
-- from (1). Operators should review counts on production snapshots before
-- running this migration. Within the typical rollout window of commit
-- 7f61740 (2026-04-18) this is safe.
--
-- Idempotent: re-running is a no-op because restored rows are no longer 0.

UPDATE memories
SET confidence = 1.0
WHERE confidence = 0
  AND id NOT IN (
    SELECT target_id FROM dream_logs
    WHERE target_type = 'memory' AND operation = 'confidence_adjusted'
  );
