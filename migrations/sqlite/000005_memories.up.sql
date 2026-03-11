-- Memories table.
CREATE TABLE memories (
  id            TEXT PRIMARY KEY,
  namespace_id  TEXT NOT NULL REFERENCES namespaces(id),
  content       TEXT NOT NULL,
  embedding_dim INTEGER,
  source        TEXT,
  tags          TEXT DEFAULT '[]',
  confidence    REAL DEFAULT 1.0,
  importance    REAL DEFAULT 0.5,
  access_count  INTEGER DEFAULT 0,
  last_accessed TEXT,
  expires_at    TEXT,
  superseded_by TEXT REFERENCES memories(id),
  enriched      INTEGER DEFAULT 0,
  metadata      TEXT DEFAULT '{}',
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  deleted_at    TEXT,
  purge_after   TEXT
);

CREATE INDEX idx_memories_namespace ON memories (namespace_id);
CREATE INDEX idx_memories_expires ON memories (expires_at);
CREATE INDEX idx_memories_enriched ON memories (namespace_id, enriched);
CREATE INDEX idx_memories_dim ON memories (embedding_dim);

-- FTS5 virtual table for full-text search.
CREATE VIRTUAL TABLE memories_fts USING fts5(memory_id, content);

-- Triggers to keep FTS in sync with the memories table.
CREATE TRIGGER memories_fts_insert AFTER INSERT ON memories BEGIN
  INSERT INTO memories_fts(memory_id, content) VALUES (NEW.id, NEW.content);
END;

CREATE TRIGGER memories_fts_delete AFTER DELETE ON memories BEGIN
  DELETE FROM memories_fts WHERE memory_id = OLD.id;
END;

CREATE TRIGGER memories_fts_update AFTER UPDATE OF content ON memories BEGIN
  DELETE FROM memories_fts WHERE memory_id = OLD.id;
  INSERT INTO memories_fts(memory_id, content) VALUES (NEW.id, NEW.content);
END;

CREATE TRIGGER memories_fts_soft_delete AFTER UPDATE OF deleted_at ON memories
  WHEN NEW.deleted_at IS NOT NULL AND OLD.deleted_at IS NULL BEGIN
  DELETE FROM memories_fts WHERE memory_id = OLD.id;
END;

CREATE TRIGGER memories_fts_soft_restore AFTER UPDATE OF deleted_at ON memories
  WHEN NEW.deleted_at IS NULL AND OLD.deleted_at IS NOT NULL BEGIN
  INSERT INTO memories_fts(memory_id, content) VALUES (NEW.id, NEW.content);
END;

-- System metadata table.
CREATE TABLE system_meta (
  key         TEXT PRIMARY KEY,
  value       TEXT NOT NULL,
  created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  updated_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
