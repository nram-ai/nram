-- Prompt-cache token accounting. SQLite mirror lives at
-- migrations/sqlite/000063.
--
-- INVARIANT: tokens_cache_read and tokens_cache_write are a SUBSET of
-- tokens_input, never an addition to it. Do NOT sum them into a total, and do
-- not add them to tokens_input in any aggregate -- that double-counts.
-- Uncached input is (tokens_input - tokens_cache_read - tokens_cache_write).
--
-- Deliberately not a CHECK constraint: the usage write path is best-effort
-- (a failed insert is logged, not retried), so a rejecting constraint would
-- discard the row whenever a provider reported inconsistent counts. The
-- recorder logs a warning instead, which keeps the evidence.
--
-- Existing rows backfill to 0, which reads as "no cache activity recorded"
-- rather than "no cache activity occurred" -- the capture code postdates them.

ALTER TABLE token_usage ADD COLUMN tokens_cache_read INTEGER NOT NULL DEFAULT 0;
ALTER TABLE token_usage ADD COLUMN tokens_cache_write INTEGER NOT NULL DEFAULT 0;
