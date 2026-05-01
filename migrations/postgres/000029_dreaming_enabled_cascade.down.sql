-- Reverse 000029: copy project.settings.dreaming_enabled back into a
-- standalone settings row, then strip dreaming_enabled from the project
-- Settings JSON. updated_by/updated_at attribution from the original rows
-- is not recoverable.

INSERT INTO settings (key, value, scope)
SELECT
  'dreaming.project.enabled',
  CASE WHEN (settings -> 'dreaming_enabled')::text = 'true' THEN '"true"' ELSE '"false"' END,
  'project:' || id::text
FROM projects
WHERE settings ? 'dreaming_enabled'
ON CONFLICT (key, scope) DO UPDATE SET value = EXCLUDED.value;

UPDATE projects
SET settings = settings - 'dreaming_enabled'
WHERE settings ? 'dreaming_enabled';
