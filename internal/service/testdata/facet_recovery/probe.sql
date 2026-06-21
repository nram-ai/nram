-- Multi-vector facet recovery probe (read-only).
--
-- Verifies that multi-vector topic facets recover the sub-topics that a single
-- pooled vector dilutes on consolidated (dream-synthesis) memories. A dream
-- synthesis pools several source memories into one memory; under a single vector a
-- multi-topic synthesis represents each individual source poorly. Multi-vector
-- facets (facet 0 = pooled whole-memory vector, facets 1..N = topic facets) are
-- meant to fix that. This probe measures the effect directly: for each synthesis,
-- it compares the worst-covered source member's cosine under the pooled vector
-- alone ("before") against the MAX over all of the synthesis's facets ("after").
-- When facets recover the diluted sub-topics, the "after" columns rise and the
-- multi-topic fraction falls.
--
-- Population (matches the before probe): dream syntheses (origin='dream') joined
-- to their source members via memory_lineage.relation='synthesized_from', keeping
-- only syntheses with >= 2 still-vectored sources. A source is "still vectored"
-- when it has a facet-0 row; purged/superseded sources (embedding_dim nulled) drop
-- out exactly as in the before probe. Source members are compared by their own
-- pooled vector (facet 0); the only thing that changes between before and after is
-- the synthesis side (pooled vs max-over-facets), isolating the facet effect.
--
-- Cosine similarity = 1 - (a <=> b), pgvector's cosine-distance operator. These
-- are exact row-to-row distances, not index-approximated nearest-neighbor scans.
--
-- Read-only: a single SELECT, no writes/DDL. Run directly with:
--   PGPASSWORD=... psql -h HOST -U USER -d DB \
--     -v ON_ERROR_STOP=1 -P pager=off -f probe.sql
-- The dim-1024 vector table is hardcoded; TestFacetRecovery_LiveData templates the
-- table name for other embedding dims (FACET_PROBE_DIM).
WITH pairs AS (
  -- synthesis<-source pairs where BOTH endpoints are still vectored at dim 1024
  SELECT l.memory_id AS syn_id, l.parent_id AS src_id
  FROM memory_lineage l
  JOIN memories sm ON sm.id = l.memory_id
    AND sm.origin = 'dream' AND sm.embedding_dim = 1024 AND sm.deleted_at IS NULL
  JOIN memories pm ON pm.id = l.parent_id
    AND pm.embedding_dim = 1024 AND pm.deleted_at IS NULL
  WHERE l.relation = 'synthesized_from'
    AND EXISTS (SELECT 1 FROM memory_vectors_1024 sv WHERE sv.memory_id = l.memory_id AND sv.facet_id = 0)
    AND EXISTS (SELECT 1 FROM memory_vectors_1024 pv WHERE pv.memory_id = l.parent_id AND pv.facet_id = 0)
),
multi AS (
  -- keep only syntheses with >= 2 distinct still-vectored sources
  SELECT syn_id FROM pairs GROUP BY syn_id HAVING count(DISTINCT src_id) >= 2
),
percpair AS (
  SELECT DISTINCT p.syn_id, p.src_id,
    -- before: synthesis pooled (facet 0) vs source pooled (facet 0)
    1 - (s0.embedding <=> src0.embedding) AS before_cos,
    -- after: max over ALL synthesis facets vs source pooled (facet 0)
    (SELECT max(1 - (sf.embedding <=> src0.embedding))
       FROM memory_vectors_1024 sf WHERE sf.memory_id = p.syn_id) AS after_cos
  FROM pairs p
  JOIN multi m ON m.syn_id = p.syn_id
  JOIN memory_vectors_1024 s0   ON s0.memory_id = p.syn_id   AND s0.facet_id = 0
  JOIN memory_vectors_1024 src0 ON src0.memory_id = p.src_id AND src0.facet_id = 0
),
persyn AS (
  -- per synthesis: worst-covered member (min cosine) before and after
  SELECT syn_id, count(*) AS n_sources, min(before_cos) AS min_before, min(after_cos) AS min_after
  FROM percpair GROUP BY syn_id
)
SELECT
  count(*)                                                                    AS syntheses,
  round(avg(n_sources)::numeric, 2)                                           AS avg_sources,
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY min_before)::numeric, 3)  AS med_min_before,
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY min_after)::numeric, 3)   AS med_min_after,
  round(percentile_cont(0.1) WITHIN GROUP (ORDER BY min_before)::numeric, 3)  AS p10_before,
  round(percentile_cont(0.1) WITHIN GROUP (ORDER BY min_after)::numeric, 3)   AS p10_after,
  round(avg((min_before < 0.45)::int)::numeric, 3)                            AS multitopic_before,
  round(avg((min_after  < 0.45)::int)::numeric, 3)                            AS multitopic_after,
  round(avg((min_before < 0.60)::int)::numeric, 3)                            AS below60_before,
  round(avg((min_after  < 0.60)::int)::numeric, 3)                            AS below60_after,
  round(avg((min_before >= 0.75)::int)::numeric, 3)                           AS tight_before,
  round(avg((min_after  >= 0.75)::int)::numeric, 3)                           AS tight_after
FROM persyn;
