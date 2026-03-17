-- Dimension-specific vector tables for memory embeddings.
-- Requires pgvector extension to be enabled by a database administrator.
-- Entity vector tables are created in 000007 after the entities table exists.
-- Tables are only created when pgvector is available; without it the server
-- operates in text-only mode (no vector similarity search).

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') THEN
    CREATE TABLE IF NOT EXISTS memory_vectors_384 (
      memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
      embedding vector(384) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_mv_384_hnsw ON memory_vectors_384 USING hnsw (embedding vector_cosine_ops);

    CREATE TABLE IF NOT EXISTS memory_vectors_512 (
      memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
      embedding vector(512) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_mv_512_hnsw ON memory_vectors_512 USING hnsw (embedding vector_cosine_ops);

    CREATE TABLE IF NOT EXISTS memory_vectors_768 (
      memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
      embedding vector(768) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_mv_768_hnsw ON memory_vectors_768 USING hnsw (embedding vector_cosine_ops);

    CREATE TABLE IF NOT EXISTS memory_vectors_1024 (
      memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
      embedding vector(1024) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_mv_1024_hnsw ON memory_vectors_1024 USING hnsw (embedding vector_cosine_ops);

    CREATE TABLE IF NOT EXISTS memory_vectors_1536 (
      memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
      embedding vector(1536) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_mv_1536_hnsw ON memory_vectors_1536 USING hnsw (embedding vector_cosine_ops);

    CREATE TABLE IF NOT EXISTS memory_vectors_3072 (
      memory_id UUID PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
      embedding vector(3072) NOT NULL
    );
    -- pgvector HNSW/IVFFlat indexes support up to 2000 dimensions;
    -- 3072-dim tables use sequential scan by default.
  END IF;
END;
$$;
