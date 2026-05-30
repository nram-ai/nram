-- One-way: the deleted non-global settings rows cannot be reconstructed, so
-- this down direction is an explicit no-op (a rollback must not error). Postgres
-- requires a boolean predicate, hence WHERE false.
SELECT 1 WHERE false;
