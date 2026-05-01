-- Migrate per-project dreaming.enabled overrides from the standalone
-- `settings` rows (key='dreaming.project.enabled', scope='project:<uuid>')
-- into each project's Settings JSON under `dreaming_enabled`. After the
-- cascade resolver lands, the scheduler reads through the project Settings
-- JSON, so the standalone rows are dead. This migration preserves any
-- explicit opt-out the user had already set, then deletes the legacy rows.
--
-- The settings.value column stores a JSON-encoded string, so the value is
-- literally '"true"' or '"false"' (quotes included). The CASE expression
-- maps that to the JSON boolean literal expected on the project side.

UPDATE projects
SET settings = json_set(
  coalesce(settings, '{}'),
  '$.dreaming_enabled',
  json(CASE WHEN s.value = '"true"' THEN 'true' ELSE 'false' END)
)
FROM settings s
WHERE s.key = 'dreaming.project.enabled'
  AND s.scope = 'project:' || projects.id;

DELETE FROM settings WHERE key = 'dreaming.project.enabled';
