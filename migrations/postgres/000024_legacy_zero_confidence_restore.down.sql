-- The restore is one-way; we cannot tell post-hoc which rows were 0 before
-- the migration without an audit table. This down migration is a no-op so
-- rollback does not error, but it cannot reproduce the original 0 values.
SELECT 1 WHERE false;
