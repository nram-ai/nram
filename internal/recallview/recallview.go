// Package recallview holds the canonical per-memory shape for the recall
// response and the single projection that produces it. Both transports (the
// REST recall handlers (internal/api) and the MCP recall tool (internal/mcp))
// serialize this exact type, so a recalled memory is byte-identical across the
// wire regardless of which surface returned it.
//
// It lives in its own leaf package because the projection needs the dream-cycle
// stamp-key constants from internal/dreaming, but internal/dreaming imports
// internal/service (so service cannot host this), and internal/api must not
// import internal/mcp (so mcp cannot host it for REST's use). recallview imports
// only service, dreaming, and model: no cycle, importable by both transports.
package recallview

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/dreaming"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// Memory is the canonical per-memory recall shape. Field order and json tags
// are the byte-identity contract: both transports marshal THIS struct, never a
// parallel copy. The intrinsic decision signals (origin, confidence,
// low_novelty, derived_from) are surfaced as typed fields; per-row audit
// bookkeeping is stripped from Metadata (it remains reachable via the REST
// detail endpoint, which returns the full model.Memory unchanged).
type Memory struct {
	ID          uuid.UUID          `json:"id"`
	ProjectSlug string             `json:"project_slug"`
	Content     string             `json:"content"`
	Tags        []string           `json:"tags"`
	Source      *string            `json:"source,omitempty"`
	Origin      model.MemoryOrigin `json:"origin"`
	Score       float64            `json:"score"`
	Confidence  float64            `json:"confidence"`
	// LowNovelty is intentionally NOT omitempty: every recalled memory reports
	// its novelty status, so a non-demoted memory emits low_novelty:false rather
	// than omitting the key.
	LowNovelty  bool            `json:"low_novelty"`
	DerivedFrom []uuid.UUID     `json:"derived_from,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}

// Options controls which bookkeeping keys survive in residual Metadata. The
// zero value strips everything (the slim default used by MCP recall). REST
// recall sets IncludeLowNovelty from its include_low_novelty request flag to
// surface the low_novelty_reason detail; IncludeAudit is reserved for an
// inspection path and is never set on recall.
type Options struct {
	IncludeLowNovelty bool
	IncludeAudit      bool
}

// lowNoveltyMetaKey is the metadata key the dream novelty audit stamps on a
// demoted memory. It is hoisted into Memory.LowNovelty and therefore always
// stripped from residual Metadata. The literal mirrors the writer in
// internal/dreaming/phase_consolidation.go.
const lowNoveltyMetaKey = "low_novelty"

// lowNoveltyReasonKey is the human-readable reason paired with a demotion.
// Unlike the bool it is detail, not a decision signal, so it stays stripped
// unless the caller opts in.
const lowNoveltyReasonKey = "low_novelty_reason"

// alwaysStrippedKeys are removed from residual Metadata on every recall,
// regardless of Options. They are pure write-path bookkeeping with no retrieval
// consumer: dream lineage (hoisted or unresolvable), the dream novelty bool
// (hoisted to LowNovelty), the consolidation/reinforce stamps, the enrichment
// ingestion-decision audit trail, and the stale one-time global->project
// migration markers. All remain reachable via the REST detail endpoint.
//
// Stamp/lineage/ingestion keys reference the constants their writers use
// (internal/model, internal/dreaming) so a rename fails to compile here. The
// migration keys have no writer (stale 2026-05-24 residue) so they cannot
// drift; TestBuildMCPRecallResponse_StripsBookkeepingMetadata exercises this
// set end-to-end via the recall projection.
var alwaysStrippedKeys = map[string]struct{}{
	// Dream lineage: cycle id is unresolvable from the recall surface;
	// source_memory_ids is hoisted into DerivedFrom.
	model.DreamMetaCycleID:         {},
	model.DreamMetaSourceMemoryIDs: {},
	// Hoisted to the typed LowNovelty field.
	lowNoveltyMetaKey: {},
	// Consolidation / reinforce per-cycle stamps (previously leaked).
	dreaming.ConsolidationLoadCheckedStampKey:   {},
	dreaming.ReinforceCheckedStampKey:           {},
	dreaming.ConsolidationClusterStampKey:       {},
	dreaming.ConsolidationClusterFingerprintKey: {},
	// Enrichment ingestion-decision audit trail (previously leaked).
	model.IngestionMetaDecision:   {},
	model.IngestionMetaDecisionAt: {},
	model.IngestionMetaTargetID:   {},
	model.IngestionMetaRationale:  {},
	model.IngestionMetaMatchCount: {},
	model.IngestionMetaTopScore:   {},
	model.IngestionMetaShadowOp:   {},
	// Stale one-time migration markers stamped on rows migrated from the
	// global project on 2026-05-24. No writer remains; pure residue.
	"migrated_from_global": {},
	"migration_date":       {},
	"original_global_id":   {},
}

// auditStampKeys are per-phase dream bookkeeping timestamps and reasons.
// Stripped on recall unless IncludeAudit (which recall never sets). Constants
// reference their writers in internal/dreaming so a rename fails to compile;
// novelty_audit_reason has no constant on the writer side (written as a
// literal) and is exercised by
// TestBuildMCPRecallResponse_StripsBookkeepingMetadata.
var auditStampKeys = map[string]struct{}{
	dreaming.ContradictionsCheckedStampKey: {},
	dreaming.NoveltyAuditStampKey:          {},
	"novelty_audit_reason":                 {},
	dreaming.ParaphraseCheckedStampKey:     {},
}

// Project maps an internal service.RecallResult to the canonical wire Memory:
// it copies the typed decision signals, hoists derived_from and low_novelty out
// of metadata, and returns metadata stripped of bookkeeping per opts.
func Project(m service.RecallResult, opts Options) Memory {
	derived, lowNovelty, residual := splitMetadata(m.Metadata, opts)
	return Memory{
		ID:          m.ID,
		ProjectSlug: m.ProjectSlug,
		Content:     m.Content,
		Tags:        m.Tags,
		Source:      m.Source,
		Origin:      m.Origin,
		Score:       m.Score,
		Confidence:  m.Confidence,
		LowNovelty:  lowNovelty,
		DerivedFrom: derived,
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
		Metadata:    residual,
	}
}

// splitMetadata hoists source_memory_ids into DerivedFrom and low_novelty into
// the typed bool, then returns the metadata residual after stripping the
// bookkeeping keys per opts. A malformed metadata blob yields (nil, false, nil);
// the projector drops it rather than passing UUIDs or noise the agent cannot
// use. The residual is re-marshaled from a map, so its keys serialize in a
// deterministic (sorted) order, which is what makes the two transports
// byte-identical.
func splitMetadata(raw json.RawMessage, opts Options) (derived []uuid.UUID, lowNovelty bool, residual json.RawMessage) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false, nil
	}

	if idsRaw, ok := obj[model.DreamMetaSourceMemoryIDs]; ok {
		var ids []string
		if err := json.Unmarshal(idsRaw, &ids); err == nil {
			for _, s := range ids {
				if id, err := uuid.Parse(s); err == nil {
					derived = append(derived, id)
				}
			}
		}
	}

	if lnRaw, ok := obj[lowNoveltyMetaKey]; ok {
		var b bool
		if err := json.Unmarshal(lnRaw, &b); err == nil {
			lowNovelty = b
		}
	}

	for k := range alwaysStrippedKeys {
		delete(obj, k)
	}
	if !opts.IncludeLowNovelty && !opts.IncludeAudit {
		delete(obj, lowNoveltyReasonKey)
	}
	if !opts.IncludeAudit {
		for k := range auditStampKeys {
			delete(obj, k)
		}
	}

	if len(obj) == 0 {
		return derived, lowNovelty, nil
	}
	cleaned, err := json.Marshal(obj)
	if err != nil {
		return derived, lowNovelty, nil
	}
	return derived, lowNovelty, cleaned
}
