-- Reverse 000032: copy each project.settings.dreaming_enabled (set as a
-- JSON boolean by the up migration) back into a standalone settings row,
-- then strip dreaming_enabled from the project Settings JSON. The down
-- direction restores enough state for the pre-cascade scheduler to keep
-- honoring the per-project flag, but cannot recover the original
-- updated_by/updated_at attribution columns — those default to current user
-- (NULL) and current time.

INSERT OR REPLACE INTO settings (key, value, scope)
SELECT
  'dreaming.project.enabled',
  CASE WHEN json_extract(settings, '$.dreaming_enabled') = 1 THEN '"true"' ELSE '"false"' END,
  'project:' || id
FROM projects
WHERE json_extract(settings, '$.dreaming_enabled') IS NOT NULL;

UPDATE projects
SET settings = json_remove(settings, '$.dreaming_enabled')
WHERE json_extract(settings, '$.dreaming_enabled') IS NOT NULL;
