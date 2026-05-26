-- Retires the cross-namespace memory-sharing feature. Project-delete
-- cascade no longer clears memory_shares rows because the table is gone.
DROP INDEX IF EXISTS idx_shares_target;
DROP TABLE IF EXISTS memory_shares;
