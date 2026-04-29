-- Rewrite legacy project.settings.ranking_weights from the old
-- {recency, relevance, importance} 3-field shape to the canonical 6-field
-- shape that matches service.RankingWeights. The only renamed field is
-- relevance → similarity; the other legacy keys (recency, importance)
-- already line up with canonical names. Any modern keys (frequency,
-- graph_relevance, confidence) are left as-is.
--
-- The parser at service.ParseRankingOverride still tolerates the legacy
-- shape, so this migration is a one-shot cleanup and not a correctness
-- prerequisite. Idempotent: re-running finds no `relevance` keys.

-- Step 1: copy `relevance` to `similarity` when similarity is unset.
-- json_extract reads from the original settings; json_remove + json_set
-- composes a single replacement value.
UPDATE projects
SET settings = json_set(
  json_remove(settings, '$.ranking_weights.relevance'),
  '$.ranking_weights.similarity',
  json_extract(settings, '$.ranking_weights.relevance')
)
WHERE
  json_extract(settings, '$.ranking_weights.relevance') IS NOT NULL
  AND json_extract(settings, '$.ranking_weights.similarity') IS NULL;

-- Step 2: drop any leftover `relevance` keys (cases where similarity
-- already existed alongside relevance — similarity wins).
UPDATE projects
SET settings = json_remove(settings, '$.ranking_weights.relevance')
WHERE json_extract(settings, '$.ranking_weights.relevance') IS NOT NULL;
