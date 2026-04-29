-- Drop the dead user.settings.ranking_weights field. The cascade for ranking
-- weights lands at project, not user; allowing user-level ranking weights
-- would persist write-only data with no observable effect on recall.
--
-- Idempotent: re-running finds no `ranking_weights` keys.
UPDATE users
SET settings = settings - 'ranking_weights'
WHERE settings ? 'ranking_weights';
