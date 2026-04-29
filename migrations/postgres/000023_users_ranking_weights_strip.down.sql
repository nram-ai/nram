-- The strip is one-way; we cannot reconstruct the dropped values. This
-- down migration is intentionally a no-op so a rollback does not error,
-- but it cannot restore the data that the up migration removed.
SELECT 1 WHERE false;
