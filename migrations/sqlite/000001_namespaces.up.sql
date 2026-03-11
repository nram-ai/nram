CREATE TABLE namespaces (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  slug        TEXT NOT NULL,
  kind        TEXT NOT NULL,
  parent_id   TEXT REFERENCES namespaces(id),
  path        TEXT NOT NULL,
  depth       INTEGER NOT NULL DEFAULT 0,
  metadata    TEXT DEFAULT '{}',
  created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  UNIQUE(parent_id, slug)
);

CREATE INDEX idx_namespaces_path ON namespaces (path);
CREATE INDEX idx_namespaces_kind ON namespaces (kind);
CREATE INDEX idx_namespaces_parent ON namespaces (parent_id);

-- Insert root namespace.
INSERT INTO namespaces (id, name, slug, kind, path, depth)
VALUES ('00000000-0000-0000-0000-000000000000', 'root', 'root', 'root', '', 0);
