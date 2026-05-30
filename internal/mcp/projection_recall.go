package mcp

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/dreaming"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/recallview"
	"github.com/nram-ai/nram/internal/service"
)

// mcpRecallMemory is the per-memory recall shape. It aliases recallview.Memory
// so the REST recall handlers and the MCP recall tool serialize the exact same
// struct (byte-identical wire shape). The reducer in result_limit.go and the
// output-schema reflection in tool_recall.go (schemaFor[mcpRecallResponse])
// both operate on this type, so adding/removing a field here flows to both the
// wire and the published tools/list output schema.
type mcpRecallMemory = recallview.Memory

// mcpRecallResponse passes service.CoverageGap through verbatim because the
// diversify_by_tag_prefix wire contract is shared with REST clients.
//
// CoverageGaps participates in the memory reducer's lockstep halving with
// memories — when coverage_gaps alone would dominate the budget on a
// diversified query, the reducer trims the tail and records a frame-
// independent kept/original ratio in Truncated.Dropped (e.g.
// "coverage_gaps_kept:5/20" meaning 5 of the original 20 gaps remain on
// the wire).
//
// Truncated has TWO writers, both meaning "this response was shrunk to fit the
// MCP token budget":
//   - newRecallReducer's buildReducedRecallResponse (result_limit.go), and
//   - the recall handler's graph pre-cap (tool_recall.go), which stamps the
//     balanced-graph kept/total sentinels so the envelope is present even when
//     the response fits without the reducer firing. When the reducer also runs,
//     buildReducedRecallResponse MERGES those pre-cap sentinels rather than
//     overwriting them (graphPreTrimmed path).
//
// No OTHER recall code may set it: stamping it on an otherwise-complete response
// misleads clients into treating a full result as partial. The field is exported
// only because encoding/json requires it.
type mcpRecallResponse struct {
	Memories     []mcpRecallMemory     `json:"memories"`
	Graph        graphResponse         `json:"graph"`
	LatencyMs    int64                 `json:"latency_ms"`
	CoverageGaps []service.CoverageGap `json:"coverage_gaps,omitempty"`
	Truncated    *truncationInfo       `json:"_truncated,omitempty"`
}

// The strip-key sets and extractDerivedFrom below now serve only the list
// (tool_list.go) and get/detail (projection_store.go) MCP tools. The recall
// path moved to internal/recallview, which keeps its own — deliberately
// broader — strip set; these are intentionally left at their prior coverage so
// list/get behavior is unchanged.

// alwaysStrippedKeys are removed from emitted metadata regardless of caller
// flags. dream_cycle_id is unresolvable from the MCP surface; source_memory_ids
// is hoisted into the typed derived_from field on the same response and would
// otherwise duplicate.
var alwaysStrippedKeys = map[string]struct{}{
	model.DreamMetaCycleID:         {},
	model.DreamMetaSourceMemoryIDs: {},
}

// lowNoveltyKeys are surfaced when include_low_novelty=true (or
// include_audit=true) on the list/get projection. They are the demotion marker
// and its *reason*. The MCP list and get tools do not expose these flags, so on
// those paths the projection always strips these keys.
var lowNoveltyKeys = map[string]struct{}{
	"low_novelty":        {},
	"low_novelty_reason": {},
}

// auditStampKeys are surfaced when include_audit=true on the REST get path.
// These are per-phase bookkeeping timestamps and reasons; they don't add value
// to recall callers but are useful for inspecting a specific memory's audit
// history. include_low_novelty does NOT un-strip these — it only un-strips
// the demotion markers themselves. The MCP get tool no longer exposes the
// include_audit flag; on that path the projection always strips these keys.
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

// projectMemory maps one recalled memory to the canonical wire shape. The
// projection lives in internal/recallview so the REST recall handler produces
// the identical struct without internal/api having to import internal/mcp.
// projectionOpts mirrors recallview.Options field-for-field; translate rather
// than alias so the two packages stay independently evolvable.
func projectMemory(m service.RecallResult, opts projectionOpts) mcpRecallMemory {
	return recallview.Project(m, recallview.Options{
		IncludeLowNovelty: opts.IncludeLowNovelty,
		IncludeAudit:      opts.IncludeAudit,
	})
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
	// The service layer drops MentionCount (it's an MCP-only presentation
	// field; adding it to service.RecallEntity would leak onto the REST recall
	// wire). Backfill it here so rankGraphSlice can use MentionCount as its
	// tertiary sort key, the same signal the graph tool uses. resolveGraphOrphans already sets it
	// on the orphan endpoints it fetches; this one batch covers the
	// originally-discovered set. Running it first means resolveGraphOrphans only
	// fetches the still-missing endpoints (a disjoint, smaller set).
	backfillMentionCounts(ctx, entityReader, entities, allowedNamespaces)
	// entities holds exactly the seed entities here (resp.Graph.Entities is
	// seed-only; resolveGraphOrphans appends edge endpoints next). Capture the
	// seed set first so rankGraphSlice can rank by hop distance from the query
	// seeds rather than by global salience.
	seedIDs := graphEntityIDSet(entities)
	entities, rels = resolveGraphOrphans(ctx, entityReader, entities, rels, allowedNamespaces)
	// Collapse relation-string variants, then rank by seed proximity with
	// per-hop-tier source diversity so the recall pre-cap (packGraphToByteBudget)
	// keeps seed-relevant, diverse edges rather than namespace hubs.
	rels = dedupGraphRelationships(rels)
	rankGraphSlice(seedIDs, entities, rels)

	return &mcpRecallResponse{
		Memories:     memories,
		Graph:        graphResponse{Entities: entities, Relationships: rels},
		LatencyMs:    resp.LatencyMs,
		CoverageGaps: resp.CoverageGaps,
	}
}
