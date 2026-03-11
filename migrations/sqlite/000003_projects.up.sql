CREATE TABLE projects (
  id                  TEXT PRIMARY KEY,
  namespace_id        TEXT NOT NULL UNIQUE REFERENCES namespaces(id),
  owner_namespace_id  TEXT NOT NULL REFERENCES namespaces(id),
  name                TEXT NOT NULL,
  slug                TEXT NOT NULL,
  description         TEXT DEFAULT '',
  default_tags        TEXT DEFAULT '[]',
  settings            TEXT DEFAULT '{}',
  created_at          TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at          TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(owner_namespace_id, slug)
);
