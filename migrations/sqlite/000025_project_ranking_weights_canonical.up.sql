-- Rewrite legacy project.settings.ranking_weights from the old
-- {recency, relevance, importance} 3-field shape to the canonical 6-field
-- shape that matches service.RankingWeights. relevance is renamed to
-- similarity; canonical similarity wins over legacy relevance when both
-- exist. service.ParseRankingOverride still tolerates the legacy shape,
-- so this is a cleanup, not a correctness prerequisite. Idempotent.

UPDATE projects
SET settings = json_set(
  json_remove(settings, '$.ranking_weights.relevance'),
  '$.ranking_weights.similarity',
  json_extract(settings, '$.ranking_weights.relevance')
)
WHERE
  json_extract(settings, '$.ranking_weights.relevance') IS NOT NULL
  AND json_extract(settings, '$.ranking_weights.similarity') IS NULL;

UPDATE projects
SET settings = json_remove(settings, '$.ranking_weights.relevance')
WHERE json_extract(settings, '$.ranking_weights.relevance') IS NOT NULL;
