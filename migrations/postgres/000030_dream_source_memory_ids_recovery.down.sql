-- The recovery is one-way; we cannot tell post-hoc which rows had their
-- source_memory_ids absent before this migration ran without an audit
-- table. This down migration is a no-op so rollback does not error, but
-- it cannot re-introduce the empty source_memory_ids state, and the
-- bug that produced it has been fixed in the same change-set, so a
-- rollback that re-introduces the bug would just produce fresh damage
-- that would be re-recovered on the next forward migration.
SELECT 1 WHERE false;
