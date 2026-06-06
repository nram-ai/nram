-- Recovery for the dream-metadata clobber bug fixed in the same change-set.
--
-- Cause: four "stale collector" optimizers in the dreaming code
-- (collectReinforceStale, collectConsolidateStale, ContradictionPhase
-- collectStale, ParaphraseDedup eligible-loop) skipped decodeMetadata when a
-- stamp marker was absent and passed an empty map[string]interface{}{}
-- downstream. The five matching stamp writers (stampReinforce,
-- stampConsolidateCluster, stampContradictionsChecked, stampParaphrase,
-- writeAuditDecision) marshalled that empty-plus-stamp map and persisted it
-- via MemoryRepo.UpdateMetadata, which is `UPDATE memories SET metadata = $1`
-- a full-column overwrite, not a JSONB merge. The first time any of those
-- phases visited a freshly-created dream synthesis, source_memory_ids and
-- dream_cycle_id were wiped. The next novelty audit then read empty source
-- IDs, fired `orphan_no_sources`, and demoted. Paraphrase dedup chained
-- the demoted versions, consolidation stayed residual every cycle, the
-- project's dirty flag never cleared, and the loop ran forever.
--
-- The Go fix prevents new damage. This migration recovers historical damage
-- from the released code that ran on every deployment in the wild.
--
-- Idempotent: every WHERE clause skips already-good rows.
--
-- String-literal cross-references (grep anchors when the Go side renames):
--   'synthesized_from'   = model.LineageSynthesizedFrom (internal/model/lineage.go)
--   'source_memory_ids'  = model.DreamMetaSourceMemoryIDs (internal/model/dream.go)
--   'dream'              = model.DreamSource (internal/model/memory.go)
--   'low_novelty*' / 'novelty_audit*' = stamps written by writeAuditDecision
--                                       (internal/dreaming/phase_consolidation.go)

-- Pass A: backfill metadata.source_memory_ids from the lineage table for
-- every dream memory whose metadata is missing the field but whose
-- memory_lineage rows are intact. Lineage rows are written transactionally
-- alongside the synthesis at create time (phase_consolidation.go), so an
-- intact lineage table is the authoritative source.
WITH parents AS (
  SELECT ml.memory_id,
         jsonb_agg(ml.parent_id::text ORDER BY ml.parent_id) AS src_ids
  FROM memory_lineage ml
  WHERE ml.parent_id IS NOT NULL
    AND ml.relation = 'synthesized_from'
  GROUP BY ml.memory_id
)
UPDATE memories m
SET metadata = COALESCE(m.metadata, '{}'::jsonb)
               || jsonb_build_object('source_memory_ids', p.src_ids)
FROM parents p
WHERE m.id = p.memory_id
  AND m.source = 'dream'
  AND m.deleted_at IS NULL
  AND NOT (COALESCE(m.metadata, '{}'::jsonb) ? 'source_memory_ids');

-- Pass B: rehabilitate dream memories that were demoted as
-- `orphan_no_sources` but whose lineage table actually has at least one
-- live parent. Strips the audit decision (so the next cycle re-evaluates),
-- restores confidence to a neutral mid-band value, and bumps updated_at so
-- the staleness predicate picks the row up next cycle. Supersession chains
-- (superseded_by) are deliberately left alone: if the rehabilitated memory
-- is genuinely a paraphrase of its successor, the next contradiction cycle
-- re-supersedes correctly; if not, the chain self-clears via the same
-- machinery that created it.
WITH live_parents AS (
  SELECT ml.memory_id, count(*) AS live_parent_count
  FROM memory_lineage ml
  JOIN memories parent ON parent.id = ml.parent_id
  WHERE ml.parent_id IS NOT NULL
    AND ml.relation = 'synthesized_from'
    AND parent.deleted_at IS NULL
  GROUP BY ml.memory_id
  HAVING count(*) > 0
)
UPDATE memories m
SET confidence = 0.5,
    metadata = (COALESCE(m.metadata, '{}'::jsonb)
                - 'low_novelty'
                - 'low_novelty_reason'
                - 'novelty_audited_at'
                - 'novelty_audit_reason'),
    updated_at = now()
FROM live_parents lp
WHERE m.id = lp.memory_id
  AND m.source = 'dream'
  AND m.deleted_at IS NULL
  AND m.metadata->>'low_novelty_reason' = 'orphan_no_sources';

-- Pass C: re-mark every project that received Pass-B writes as dirty so
-- the dream scheduler picks it up promptly. A bulk SQL UPDATE does not
-- flow through the in-process DirtyTracker event bus, so without this
-- the scheduler would only re-queue affected projects on the next user
-- write. The COALESCE preserves any existing dirty_since (we never reset
-- a project that's already dirtier than the rehab point).
INSERT INTO dream_project_dirty (project_id, dirty_since, last_dream_at)
SELECT p.id, now(), NULL
FROM projects p
JOIN namespaces project_ns ON project_ns.id = p.namespace_id
WHERE EXISTS (
  SELECT 1 FROM memories m
  JOIN namespaces mem_ns ON mem_ns.id = m.namespace_id
  WHERE m.source = 'dream'
    AND m.deleted_at IS NULL
    AND m.updated_at >= now() - interval '5 minutes'
    AND (mem_ns.id = project_ns.id OR mem_ns.path LIKE project_ns.path || '/%')
)
ON CONFLICT (project_id) DO UPDATE
  SET dirty_since = COALESCE(dream_project_dirty.dirty_since, EXCLUDED.dirty_since);
