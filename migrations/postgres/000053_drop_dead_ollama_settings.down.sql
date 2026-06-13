-- One-way: the deleted settings rows were dead no-op knobs and cannot be
-- meaningfully reconstructed, so this down direction is an explicit no-op (a
-- rollback must not error). Mirrors the 000045_settings_strip_nonglobal_scope
-- precedent. Postgres requires a boolean predicate, hence WHERE false.
SELECT 1 WHERE false;
