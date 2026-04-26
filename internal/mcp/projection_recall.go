package mcp

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
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

// dreamMetaKeys are stripped from emitted metadata: dream_cycle_id is
// unresolvable from the MCP surface, and source_memory_ids is replaced by the
// typed derived_from field.
var dreamMetaKeys = map[string]struct{}{
	model.DreamMetaCycleID:         {},
	model.DreamMetaSourceMemoryIDs: {},
}

// extractDerivedFrom plucks source_memory_ids into a typed slice and returns
// the metadata residual after stripping dream-only keys. Invalid blobs return
// (nil, nil) — the projector drops them rather than passing UUIDs the agent
// can't resolve.
func extractDerivedFrom(raw json.RawMessage) (derived []uuid.UUID, residual json.RawMessage) {
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

	for k := range dreamMetaKeys {
		delete(obj, k)
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

func projectMemory(m service.RecallResult) mcpRecallMemory {
	derived, meta := extractDerivedFrom(m.Metadata)
	return mcpRecallMemory{
		ID:          m.ID,
		ProjectSlug: m.ProjectSlug,
		Content:     m.Content,
		Tags:        m.Tags,
		Source:      m.Source,
		Score:       m.Score,
		CreatedAt:   m.CreatedAt,
		DerivedFrom: derived,
		Metadata:    meta,
	}
}

// buildMCPRecallResponse projects a service.RecallResponse into the MCP shape,
// hoisting dream lineage and routing the graph through resolveGraphOrphans so
// the response never contains an edge whose endpoint isn't in entities[].
func buildMCPRecallResponse(
	ctx context.Context,
	entityReader EntityReader,
	resp *service.RecallResponse,
	allowedNamespaces []uuid.UUID,
) *mcpRecallResponse {
	memories := make([]mcpRecallMemory, 0, len(resp.Memories))
	for _, m := range resp.Memories {
		memories = append(memories, projectMemory(m))
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
