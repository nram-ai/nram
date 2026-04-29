-- Reverse: copy `similarity` back to `relevance` for projects whose
-- ranking_weights still resemble the legacy 3-field shape (no frequency,
-- graph_relevance, or confidence keys). Projects with canonical-shape
-- overrides are left alone — there is no faithful inverse for them.
UPDATE projects
SET settings = json_set(
  json_remove(settings, '$.ranking_weights.similarity'),
  '$.ranking_weights.relevance',
  json_extract(settings, '$.ranking_weights.similarity')
)
WHERE
  json_extract(settings, '$.ranking_weights.similarity') IS NOT NULL
  AND json_extract(settings, '$.ranking_weights.frequency') IS NULL
  AND json_extract(settings, '$.ranking_weights.graph_relevance') IS NULL
  AND json_extract(settings, '$.ranking_weights.confidence') IS NULL;
