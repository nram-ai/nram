-- Rewrite legacy project.settings.ranking_weights to the canonical 6-field
-- shape. Only `relevance → similarity` needs renaming; canonical similarity
-- wins over legacy relevance when both exist. Idempotent.

UPDATE projects
SET settings = jsonb_set(
  settings #- '{ranking_weights,relevance}',
  '{ranking_weights,similarity}',
  settings #> '{ranking_weights,relevance}'
)
WHERE
  settings #> '{ranking_weights,relevance}' IS NOT NULL
  AND settings #> '{ranking_weights,similarity}' IS NULL;

UPDATE projects
SET settings = settings #- '{ranking_weights,relevance}'
WHERE settings #> '{ranking_weights,relevance}' IS NOT NULL;
