package model

import "encoding/json"

// Ingestion* are the metadata keys the enrichment ingestion phase stamps on a
// memory to record its dedup/ADD decision and the evidence behind it. They live
// here, next to the dream metadata keys (DreamMeta*), so the writer
// (internal/enrichment) and any reader that strips or inspects them
// (internal/recallview) share one definition and a rename is caught at compile
// time rather than drifting silently between packages.
const (
	IngestionMetaDecision   = "ingestion_decision"
	IngestionMetaDecisionAt = "ingestion_decision_at"
	IngestionMetaTargetID   = "ingestion_target_id"
	IngestionMetaRationale  = "ingestion_rationale"
	IngestionMetaMatchCount = "ingestion_match_count"
	IngestionMetaTopScore   = "ingestion_top_score"
	IngestionMetaShadowOp   = "ingestion_shadow_op"
)

// MetaLowNovelty is the metadata key the dream novelty audit stamps on a memory
// it has demoted as low-novelty. Defined here so its one reader
// (internal/recallview, which hoists it into the typed LowNovelty field) and any
// path that strips lineage provenance (SanitizeRelocatedMetadata) share a single
// definition.
const MetaLowNovelty = "low_novelty"

// relocatedProvenanceKeys are the machine-generated provenance keys that are
// meaningful only inside the namespace that produced them and become invalid
// (or actively misleading) when a memory is relocated to another project's
// namespace, so they must be dropped on a move:
//   - DreamMetaCycleID / DreamMetaSourceMemoryIDs / MetaLowNovelty: dream-lineage
//     derivation metadata. source_memory_ids is re-projected as derived_from on
//     recall; all three point at the source namespace's dream cycle/memories.
//   - IngestionMetaTargetID: the memory this row deduped/updated against at
//     ingestion. It is a memory-ID reference into the source namespace, so after
//     a move it dangles at a foreign-namespace memory (and is user-visible via
//     the REST detail endpoint, which returns metadata unstripped).
//
// The other IngestionMeta* keys (decision, decision_at, rationale, match_count,
// top_score, shadow_op) are deliberately kept: they are scalar audit values
// describing what happened at ingestion, not cross-namespace references, so they
// remain historically meaningful after a move.
var relocatedProvenanceKeys = []string{
	DreamMetaCycleID,
	DreamMetaSourceMemoryIDs,
	MetaLowNovelty,
	IngestionMetaTargetID,
}

// SanitizeRelocatedMetadata returns a copy of raw with the dream-lineage
// provenance keys removed, for use when a memory is re-stored into a different
// project's namespace (a move). The relocated copy is a fresh user-origin
// memory: its source_memory_ids point at memories living in the old namespace,
// so carrying them verbatim would make the copy advertise dangling
// cross-namespace provenance (surfaced as derived_from on recall). Empty or
// malformed input, or input carrying none of the stripped keys, is returned
// unchanged so callers can hand the result straight to a store request.
func SanitizeRelocatedMetadata(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	stripped := false
	for _, k := range relocatedProvenanceKeys {
		if _, ok := obj[k]; ok {
			delete(obj, k)
			stripped = true
		}
	}
	if !stripped {
		return raw
	}
	sanitized, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return sanitized
}
