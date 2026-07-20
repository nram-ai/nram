-- DROP COLUMN requires SQLite 3.35+ (2021-03). The driver is
-- modernc.org/sqlite, well past that, so the 000023/000034 table-rebuild
-- pattern is unnecessary here.

ALTER TABLE token_usage DROP COLUMN tokens_cache_write;
ALTER TABLE token_usage DROP COLUMN tokens_cache_read;
