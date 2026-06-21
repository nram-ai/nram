-- Reverse 000057: drop topic facets and restore the single-vector (memory_id)
-- primary key on memory_vectors.
CREATE TABLE memory_vectors_new (
    memory_id TEXT PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
    namespace_id TEXT NOT NULL REFERENCES namespaces(id),
    dimension INTEGER NOT NULL,
    embedding BLOB NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

INSERT INTO memory_vectors_new (memory_id, namespace_id, dimension, embedding, created_at, updated_at)
    SELECT memory_id, namespace_id, dimension, embedding, created_at, updated_at FROM memory_vectors WHERE facet_id = 0;

DROP TABLE memory_vectors;
ALTER TABLE memory_vectors_new RENAME TO memory_vectors;
CREATE INDEX IF NOT EXISTS idx_memory_vectors_ns_dim ON memory_vectors(namespace_id, dimension);
