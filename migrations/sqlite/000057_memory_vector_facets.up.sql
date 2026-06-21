-- Add facet_id to memory_vectors so a memory can carry multiple vectors:
-- facet 0 is the pooled whole-memory embedding (every existing row becomes
-- facet 0) and facets 1..N are topic facets. The primary key becomes
-- (memory_id, facet_id). SQLite requires a table rebuild to change the key.
-- Mirrors the rebuild pattern used in 000013.
CREATE TABLE memory_vectors_new (
    memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
    facet_id INTEGER NOT NULL DEFAULT 0,
    namespace_id TEXT NOT NULL REFERENCES namespaces(id),
    dimension INTEGER NOT NULL,
    embedding BLOB NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (memory_id, facet_id)
);

INSERT INTO memory_vectors_new (memory_id, facet_id, namespace_id, dimension, embedding, created_at, updated_at)
    SELECT memory_id, 0, namespace_id, dimension, embedding, created_at, updated_at FROM memory_vectors;

DROP TABLE memory_vectors;
ALTER TABLE memory_vectors_new RENAME TO memory_vectors;
-- facet_id is part of the index so the graph rebuild (facet_id = 0) and the
-- topic-facet brute-force scan (facet_id > 0) are both index-resolved.
CREATE INDEX IF NOT EXISTS idx_memory_vectors_ns_dim ON memory_vectors(namespace_id, dimension, facet_id);
