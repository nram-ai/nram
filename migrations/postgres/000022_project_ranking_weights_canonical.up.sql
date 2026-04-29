-- Rewrite legacy project.settings.ranking_weights from the old
-- {recency, relevance, importance} 3-field shape to the canonical 6-field
-- shape that matches service.RankingWeights. Only `relevance → similarity`
-- needs renaming; other legacy keys already line up.
--
-- Idempotent: re-running finds no `relevance` keys.

-- Step 1: copy `relevance` to `similarity` when similarity is unset.
UPDATE projects
SET settings = jsonb_set(
  settings #- '{ranking_weights,relevance}',
  '{ranking_weights,similarity}',
  settings #> '{ranking_weights,relevance}'
)
WHERE
  settings #> '{ranking_weights,relevance}' IS NOT NULL
  AND settings #> '{ranking_weights,similarity}' IS NULL;

-- Step 2: drop any leftover `relevance` keys.
UPDATE projects
SET settings = settings #- '{ranking_weights,relevance}'
WHERE settings #> '{ranking_weights,relevance}' IS NOT NULL;
