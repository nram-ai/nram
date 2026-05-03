-- SQLite mirror of migrations/postgres/000030. Same Pass A / Pass B / Pass C
-- structure; SQLite uses json_patch / json_remove / json_group_array /
-- json_object instead of Postgres's `||` / `-` / jsonb_agg / jsonb_build_object.
-- See the Postgres file for the bug background, idempotency notes, and
-- the string-literal cross-references to the Go constants.

-- Pass A: backfill metadata.source_memory_ids from the lineage table for
-- every dream memory whose metadata is missing the field but whose
-- memory_lineage rows are intact.
UPDATE memories
SET metadata = json_patch(
  COALESCE(metadata, '{}'),
  json_object('source_memory_ids', (
    SELECT json_group_array(parent_id)
    FROM memory_lineage ml
    WHERE ml.memory_id = memories.id
      AND ml.relation = 'synthesized_from'
      AND ml.parent_id IS NOT NULL
  ))
)
WHERE source = 'dream'
  AND deleted_at IS NULL
  AND json_extract(COALESCE(metadata, '{}'), '$.source_memory_ids') IS NULL
  AND EXISTS (
    SELECT 1 FROM memory_lineage ml
    WHERE ml.memory_id = memories.id
      AND ml.relation = 'synthesized_from'
      AND ml.parent_id IS NOT NULL
  );

-- Pass B: rehabilitate dream memories that were demoted as
-- `orphan_no_sources` but whose lineage table actually has at least one
-- live parent. Strips the audit decision (so the next cycle re-evaluates),
-- restores confidence to a neutral mid-band value, and bumps updated_at so
-- the staleness predicate picks the row up next cycle.
UPDATE memories
SET confidence = 0.5,
    metadata = json_remove(
      json_remove(
        json_remove(
          json_remove(COALESCE(metadata, '{}'),
            '$.low_novelty'),
          '$.low_novelty_reason'),
        '$.novelty_audited_at'),
      '$.novelty_audit_reason'),
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE source = 'dream'
  AND deleted_at IS NULL
  AND json_extract(metadata, '$.low_novelty_reason') = 'orphan_no_sources'
  AND EXISTS (
    SELECT 1 FROM memory_lineage ml
    JOIN memories parent ON parent.id = ml.parent_id
    WHERE ml.memory_id = memories.id
      AND ml.relation = 'synthesized_from'
      AND ml.parent_id IS NOT NULL
      AND parent.deleted_at IS NULL
  );

-- Pass C: re-mark every project that received Pass-B writes as dirty so
-- the dream scheduler picks it up promptly. SQLite's UPSERT syntax
-- mirrors Postgres's. The COALESCE preserves any existing dirty_since.
INSERT INTO dream_project_dirty (project_id, dirty_since, last_dream_at)
SELECT p.id, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), NULL
FROM projects p
JOIN namespaces project_ns ON project_ns.id = p.namespace_id
WHERE EXISTS (
  SELECT 1 FROM memories m
  JOIN namespaces mem_ns ON mem_ns.id = m.namespace_id
  WHERE m.source = 'dream'
    AND m.deleted_at IS NULL
    AND m.updated_at >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-5 minutes')
    AND (mem_ns.id = project_ns.id OR mem_ns.path LIKE project_ns.path || '/%')
)
ON CONFLICT (project_id) DO UPDATE
  SET dirty_since = COALESCE(dream_project_dirty.dirty_since, excluded.dirty_since);
