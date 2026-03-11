-- Entities table (knowledge graph nodes).
CREATE TABLE entities (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id  UUID NOT NULL REFERENCES namespaces(id),
  name          TEXT NOT NULL,
  canonical     TEXT NOT NULL,
  entity_type   TEXT NOT NULL,
  embedding_dim INT,
  properties    JSONB DEFAULT '{}',
  mention_count INT DEFAULT 1,
  metadata      JSONB DEFAULT '{}',
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now(),
  UNIQUE(namespace_id, canonical, entity_type)
);

CREATE INDEX idx_entities_namespace ON entities (namespace_id);
CREATE INDEX idx_entities_type ON entities (namespace_id, entity_type);

-- Entity vector tables (deferred from 000006 due to FK dependency on entities).
CREATE TABLE entity_vectors_384 (
  entity_id UUID PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
  embedding vector(384) NOT NULL
);
CREATE INDEX idx_ev_384_hnsw ON entity_vectors_384 USING hnsw (embedding vector_cosine_ops);

CREATE TABLE entity_vectors_512 (
  entity_id UUID PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
  embedding vector(512) NOT NULL
);
CREATE INDEX idx_ev_512_hnsw ON entity_vectors_512 USING hnsw (embedding vector_cosine_ops);

CREATE TABLE entity_vectors_768 (
  entity_id UUID PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
  embedding vector(768) NOT NULL
);
CREATE INDEX idx_ev_768_hnsw ON entity_vectors_768 USING hnsw (embedding vector_cosine_ops);

CREATE TABLE entity_vectors_1024 (
  entity_id UUID PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
  embedding vector(1024) NOT NULL
);
CREATE INDEX idx_ev_1024_hnsw ON entity_vectors_1024 USING hnsw (embedding vector_cosine_ops);

CREATE TABLE entity_vectors_1536 (
  entity_id UUID PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
  embedding vector(1536) NOT NULL
);
CREATE INDEX idx_ev_1536_hnsw ON entity_vectors_1536 USING hnsw (embedding vector_cosine_ops);

CREATE TABLE entity_vectors_3072 (
  entity_id UUID PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
  embedding vector(3072) NOT NULL
);
CREATE INDEX idx_ev_3072_hnsw ON entity_vectors_3072 USING hnsw (embedding vector_cosine_ops);

-- Relationships table (knowledge graph edges).
CREATE TABLE relationships (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id    UUID NOT NULL REFERENCES namespaces(id),
  source_id       UUID NOT NULL REFERENCES entities(id),
  target_id       UUID NOT NULL REFERENCES entities(id),
  relation        TEXT NOT NULL,
  weight          FLOAT DEFAULT 1.0,
  properties      JSONB DEFAULT '{}',
  valid_from      TIMESTAMPTZ DEFAULT now(),
  valid_until     TIMESTAMPTZ,
  source_memory   UUID REFERENCES memories(id),
  created_at      TIMESTAMPTZ DEFAULT now(),
  UNIQUE(namespace_id, source_id, target_id, relation, valid_from)
);

CREATE INDEX idx_relationships_source
  ON relationships (source_id) WHERE valid_until IS NULL;
CREATE INDEX idx_relationships_target
  ON relationships (target_id) WHERE valid_until IS NULL;
CREATE INDEX idx_relationships_namespace
  ON relationships (namespace_id);

-- Entity aliases table (coreference resolution).
CREATE TABLE entity_aliases (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id     UUID NOT NULL REFERENCES entities(id),
  alias         TEXT NOT NULL,
  alias_type    TEXT DEFAULT 'name',
  created_at    TIMESTAMPTZ DEFAULT now(),
  UNIQUE(entity_id, alias)
);

CREATE INDEX idx_aliases_alias ON entity_aliases (alias);

-- Memory lineage table (evolution tracking).
CREATE TABLE memory_lineage (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  memory_id     UUID NOT NULL REFERENCES memories(id),
  parent_id     UUID REFERENCES memories(id),
  relation      TEXT NOT NULL,
  context       JSONB DEFAULT '{}',
  created_at    TIMESTAMPTZ DEFAULT now()
);
