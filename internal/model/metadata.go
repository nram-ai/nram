package model

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
