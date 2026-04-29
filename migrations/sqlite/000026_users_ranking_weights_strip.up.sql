-- Drop the dead user.settings.ranking_weights field. The cascade for ranking
-- weights lands at project, not user; allowing user-level ranking weights
-- would persist write-only data with no observable effect on recall. Other
-- user settings keys (dedup_threshold, enrichment_enabled, etc.) are left
-- untouched.
--
-- Idempotent: re-running finds no `ranking_weights` keys.
UPDATE users
SET settings = json_remove(settings, '$.ranking_weights')
WHERE json_extract(settings, '$.ranking_weights') IS NOT NULL;
