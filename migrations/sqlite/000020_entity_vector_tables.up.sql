CREATE TABLE IF NOT EXISTS entity_vectors (
    entity_id TEXT PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
    namespace_id TEXT NOT NULL REFERENCES namespaces(id),
    dimension INTEGER NOT NULL,
    embedding BLOB NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_entity_vectors_ns_dim ON entity_vectors(namespace_id, dimension);

CREATE TABLE IF NOT EXISTS entity_hnsw_snapshots (
    namespace_id TEXT NOT NULL REFERENCES namespaces(id),
    dimension INTEGER NOT NULL,
    graph_data BLOB NOT NULL,
    node_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (namespace_id, dimension)
);
