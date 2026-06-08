-- No-op: the up migration deletes corrupt cross-namespace relationship rows.
-- They are invalid by construction and intentionally unrecoverable, so there is
-- nothing to restore on a rollback.
SELECT 1;
