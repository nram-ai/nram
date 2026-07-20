-- Prompt-cache token accounting. See migrations/postgres/000063 for the
-- invariant these two columns carry.
--
-- No table rebuild is needed here, unlike migrations/sqlite/000023 and 000034:
-- those rebuilt token_usage to change a foreign-key action, which SQLite cannot
-- alter in place. A plain column add is supported directly.
--
-- If a future migration does rebuild this table, both columns must be carried
-- in its INSERT INTO token_usage_new (...) SELECT ... copy list.

ALTER TABLE token_usage ADD COLUMN tokens_cache_read INTEGER NOT NULL DEFAULT 0;
ALTER TABLE token_usage ADD COLUMN tokens_cache_write INTEGER NOT NULL DEFAULT 0;
