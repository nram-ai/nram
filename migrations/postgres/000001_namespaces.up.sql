CREATE TABLE namespaces (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT NOT NULL,
  slug        TEXT NOT NULL,
  kind        TEXT NOT NULL,
  parent_id   UUID REFERENCES namespaces(id),
  path        TEXT NOT NULL,
  depth       INT NOT NULL DEFAULT 0,
  metadata    JSONB DEFAULT '{}',
  created_at  TIMESTAMPTZ DEFAULT now(),
  updated_at  TIMESTAMPTZ DEFAULT now(),
  UNIQUE(parent_id, slug)
);

CREATE INDEX idx_namespaces_path ON namespaces (path);
CREATE INDEX idx_namespaces_path_prefix ON namespaces (path text_pattern_ops);
CREATE INDEX idx_namespaces_kind ON namespaces (kind);
CREATE INDEX idx_namespaces_parent ON namespaces (parent_id);

-- Insert root namespace.
INSERT INTO namespaces (id, name, slug, kind, path, depth)
VALUES ('00000000-0000-0000-0000-000000000000', 'root', 'root', 'root', '', 0);
