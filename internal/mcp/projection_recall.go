package mcp

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/dreaming"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// mcpRecallMemory hoists source_memory_ids from metadata into derived_from so
// the agent gets a typed lineage pointer (each id is fetchable via memory_get)
// instead of a buried UUID blob.
type mcpRecallMemory struct {
	ID          uuid.UUID       `json:"id"`
	ProjectSlug string          `json:"project_slug"`
	Content     string          `json:"content"`
	Tags        []string        `json:"tags"`
	Source      *string         `json:"source,omitempty"`
	Score       float64         `json:"score"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	DerivedFrom []uuid.UUID     `json:"derived_from,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}

// mcpRecallResponse passes service.CoverageGap through verbatim because the
// diversify_by_tag_prefix wire contract is shared with REST clients.
type mcpRecallResponse struct {
	Memories     []mcpRecallMemory     `json:"memories"`
	Graph        graphResponse         `json:"graph"`
	LatencyMs    int64                 `json:"latency_ms"`
	CoverageGaps []service.CoverageGap `json:"coverage_gaps,omitempty"`
}

// alwaysStrippedKeys are removed from emitted metadata regardless of caller
// flags. dream_cycle_id is unresolvable from the MCP surface; source_memory_ids
// is hoisted into the typed derived_from field on the same response and would
// otherwise duplicate.
var alwaysStrippedKeys = map[string]struct{}{
	model.DreamMetaCycleID:         {},
	model.DreamMetaSourceMemoryIDs: {},
}

// lowNoveltyKeys are surfaced when include_low_novelty=true on memory_recall
// (and when include_audit=true on memory_get). They are the *reason* a dream
// was demoted, paired with the demoted memory itself.
var lowNoveltyKeys = map[string]struct{}{
	"low_novelty":        {},
	"low_novelty_reason": {},
}

// auditStampKeys are surfaced when include_audit=true on memory_get. These are
// per-phase bookkeeping timestamps and reasons; they don't add value to recall
// callers but are useful for inspecting a specific memory's audit history.
// include_low_novelty does NOT un-strip these — it only un-strips the demotion
// markers themselves.
//
// Stamp-key constants live with their writers in internal/dreaming so a rename
// fails to compile here. novelty_audit_reason has no constant on the writer
// side yet (it's written as a literal); a CI test catches that drift.
var auditStampKeys = map[string]struct{}{
	dreaming.ContradictionsCheckedStampKey: {},
	dreaming.NoveltyAuditStampKey:          {},
	"novelty_audit_reason":                 {},
	dreaming.ParaphraseCheckedStampKey:     {},
}

// projectionOpts controls which bookkeeping keys are surfaced in MCP responses.
// Default zero-value strips everything (preserving pre-existing behavior); set
// IncludeLowNovelty or IncludeAudit to expose the corresponding key sets.
type projectionOpts struct {
	IncludeLowNovelty bool
	IncludeAudit      bool
}

// extractDerivedFrom plucks source_memory_ids into a typed slice and returns
// the metadata residual after stripping bookkeeping keys. Invalid blobs return
// (nil, nil) — the projector drops them rather than passing UUIDs the agent
// can't resolve. opts controls which key subsets survive the strip.
func extractDerivedFrom(raw json.RawMessage, opts projectionOpts) (derived []uuid.UUID, residual json.RawMessage) {
	if len(raw) == 0 {
		return nil, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, nil
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

	for k := range alwaysStrippedKeys {
		delete(obj, k)
	}
	if !opts.IncludeLowNovelty && !opts.IncludeAudit {
		for k := range lowNoveltyKeys {
			delete(obj, k)
		}
	}
	if !opts.IncludeAudit {
		for k := range auditStampKeys {
			delete(obj, k)
		}
	}
	if len(obj) == 0 {
		return derived, nil
	}
	cleaned, err := json.Marshal(obj)
	if err != nil {
		return derived, nil
	}
	return derived, cleaned
}

func projectMemory(m service.RecallResult, opts projectionOpts) mcpRecallMemory {
	derived, meta := extractDerivedFrom(m.Metadata, opts)
	return mcpRecallMemory{
		ID:          m.ID,
		ProjectSlug: m.ProjectSlug,
		Content:     m.Content,
		Tags:        m.Tags,
		Source:      m.Source,
		Score:       m.Score,
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
		DerivedFrom: derived,
		Metadata:    meta,
	}
}

// buildMCPRecallResponse projects a service.RecallResponse into the MCP shape,
// hoisting dream lineage and routing the graph through resolveGraphOrphans so
// the response never contains an edge whose endpoint isn't in entities[].
// opts controls which bookkeeping keys survive the strip — caller passes the
// parsed include_low_novelty flag through.
func buildMCPRecallResponse(
	ctx context.Context,
	entityReader EntityReader,
	resp *service.RecallResponse,
	allowedNamespaces []uuid.UUID,
	opts projectionOpts,
) *mcpRecallResponse {
	memories := make([]mcpRecallMemory, 0, len(resp.Memories))
	for _, m := range resp.Memories {
		memories = append(memories, projectMemory(m, opts))
	}

	entities := make([]graphEntity, 0, len(resp.Graph.Entities))
	for _, e := range resp.Graph.Entities {
		entities = append(entities, graphEntity{ID: e.ID, Name: e.Name, Type: e.EntityType})
	}
	rels := make([]graphRelationship, 0, len(resp.Graph.Relationships))
	for _, r := range resp.Graph.Relationships {
		rels = append(rels, graphRelationship{
			SourceID: r.SourceID,
			TargetID: r.TargetID,
			Relation: r.Relation,
			Weight:   r.Weight,
		})
	}
	entities, rels = resolveGraphOrphans(ctx, entityReader, entities, rels, allowedNamespaces)

	return &mcpRecallResponse{
		Memories:     memories,
		Graph:        graphResponse{Entities: entities, Relationships: rels},
		LatencyMs:    resp.LatencyMs,
		CoverageGaps: resp.CoverageGaps,
	}
}
