-- Revert FK constraints on memory_vectors and hnsw_snapshots (remove FK on namespace_id).

CREATE TABLE memory_vectors_old (
    memory_id TEXT PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
    namespace_id TEXT NOT NULL,
    dimension INTEGER NOT NULL,
    embedding BLOB NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

INSERT INTO memory_vectors_old SELECT * FROM memory_vectors;
DROP TABLE memory_vectors;
ALTER TABLE memory_vectors_old RENAME TO memory_vectors;
CREATE INDEX IF NOT EXISTS idx_memory_vectors_ns_dim ON memory_vectors(namespace_id, dimension);

CREATE TABLE hnsw_snapshots_old (
    namespace_id TEXT NOT NULL,
    dimension INTEGER NOT NULL,
    graph_data BLOB NOT NULL,
    node_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (namespace_id, dimension)
);

INSERT INTO hnsw_snapshots_old SELECT * FROM hnsw_snapshots;
DROP TABLE hnsw_snapshots;
ALTER TABLE hnsw_snapshots_old RENAME TO hnsw_snapshots;
