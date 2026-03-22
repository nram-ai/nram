CREATE TABLE IF NOT EXISTS memory_vectors (
    memory_id TEXT PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
    namespace_id TEXT NOT NULL,
    dimension INTEGER NOT NULL,
    embedding BLOB NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_vectors_ns_dim ON memory_vectors(namespace_id, dimension);

CREATE TABLE IF NOT EXISTS hnsw_snapshots (
    namespace_id TEXT NOT NULL,
    dimension INTEGER NOT NULL,
    graph_data BLOB NOT NULL,
    node_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (namespace_id, dimension)
);
