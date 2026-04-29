-- Reverse: copy `similarity` back to `relevance` for projects whose
-- ranking_weights still resemble the legacy 3-field shape. Projects with
-- canonical-shape overrides are left alone — no faithful inverse exists.
UPDATE projects
SET settings = jsonb_set(
  settings #- '{ranking_weights,similarity}',
  '{ranking_weights,relevance}',
  settings #> '{ranking_weights,similarity}'
)
WHERE
  settings #> '{ranking_weights,similarity}' IS NOT NULL
  AND settings #> '{ranking_weights,frequency}' IS NULL
  AND settings #> '{ranking_weights,graph_relevance}' IS NULL
  AND settings #> '{ranking_weights,confidence}' IS NULL;
