CREATE TABLE projects (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id        UUID NOT NULL UNIQUE REFERENCES namespaces(id),
  owner_namespace_id  UUID NOT NULL REFERENCES namespaces(id),
  name                TEXT NOT NULL,
  slug                TEXT NOT NULL,
  description         TEXT DEFAULT '',
  default_tags        TEXT[] DEFAULT '{}',
  settings            JSONB DEFAULT '{}',
  created_at          TIMESTAMPTZ DEFAULT now(),
  updated_at          TIMESTAMPTZ DEFAULT now(),
  UNIQUE(owner_namespace_id, slug)
);
