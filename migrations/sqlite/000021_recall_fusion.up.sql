-- Hybrid recall: covering index for the FTS-join filter. The
-- memories_fts FTS5 virtual table and its 5 sync triggers were
-- created in 000005; recall's lexical channel queries it joined to
-- memories for namespace + soft-delete filtering. This index lets
-- that join resolve without a per-row primary-key probe.
CREATE INDEX IF NOT EXISTS idx_memories_id_ns_live
  ON memories (id, namespace_id) WHERE deleted_at IS NULL;
