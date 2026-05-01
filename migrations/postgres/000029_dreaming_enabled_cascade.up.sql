-- Migrate per-project dreaming.enabled overrides from standalone settings
-- rows (key='dreaming.project.enabled', scope='project:<uuid>') into each
-- project's Settings JSON under `dreaming_enabled`. See SQLite mirror
-- (migrations/sqlite/000032) for context.
--
-- settings.value stores a JSON-encoded string ('"true"' / '"false"'). We
-- map that to the canonical JSON boolean expected by the cascade.

UPDATE projects p
SET settings = jsonb_set(
  coalesce(p.settings, '{}'::jsonb),
  '{dreaming_enabled}',
  CASE WHEN s.value = '"true"' THEN 'true'::jsonb ELSE 'false'::jsonb END,
  true
)
FROM settings s
WHERE s.key = 'dreaming.project.enabled'
  AND s.scope = 'project:' || p.id::text;

DELETE FROM settings WHERE key = 'dreaming.project.enabled';
