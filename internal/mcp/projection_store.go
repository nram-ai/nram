package mcp

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// mcpStoreResponse mirrors service.StoreResponse. Enriched is a hard false on
// the insert path (store.go) because enrichment is queued, not inline; the dedup
// path passes the existing memory's value through and queues nothing.
type mcpStoreResponse struct {
	ID               uuid.UUID `json:"id"`
	ProjectSlug      string    `json:"project_slug"`
	Enriched         bool      `json:"enriched" jsonschema_description:"Whether entity and relationship extraction has already run for this memory. ALWAYS false on a new insert because enrichment runs later; false is not a partial failure and does not warrant a retry."`
	EnrichmentQueued bool      `json:"enrichment_queued,omitempty" jsonschema_description:"True when this insert was enqueued for enrichment. Absent on the dedup path, where an existing memory was returned and nothing needed queueing; absence does not mean enrichment is broken."`
}

func buildMCPStoreResponse(resp *service.StoreResponse) *mcpStoreResponse {
	return &mcpStoreResponse{
		ID:               resp.ID,
		ProjectSlug:      resp.ProjectSlug,
		Enriched:         resp.Enriched,
		EnrichmentQueued: resp.EnrichmentQueued,
	}
}

type mcpBatchStoreResponse struct {
	Processed       int                       `json:"processed"`
	MemoriesCreated int                       `json:"memories_created"`
	Errors          []service.BatchStoreError `json:"errors"`
}

func buildMCPBatchStoreResponse(resp *service.BatchStoreResponse) *mcpBatchStoreResponse {
	return &mcpBatchStoreResponse{
		Processed:       resp.Processed,
		MemoriesCreated: resp.MemoriesCreated,
		Errors:          resp.Errors,
	}
}

// mcpUpdateResponse mirrors service.UpdateResponse; the ID and Superseded field
// descriptions carry the copy-on-write contract. The case they do not cover: a
// tags/metadata-only update edits in place and leaves ID unchanged.
type mcpUpdateResponse struct {
	ID               uuid.UUID `json:"id" jsonschema_description:"The memory id after the update. A content change writes a NEW row, so this differs from the id you sent; use this one afterwards or you target a superseded row."`
	PreviousMemoryID uuid.UUID `json:"previous_memory_id"`
	ReEmbedded       bool      `json:"re_embedded"`
	Superseded       bool      `json:"superseded" jsonschema_description:"True when the content change wrote a new row and retired the old one. It reports that the copy-on-write path succeeded, not that your memory was invalidated."`
}

func buildMCPUpdateResponse(resp *service.UpdateResponse) *mcpUpdateResponse {
	return &mcpUpdateResponse{
		ID:               resp.ID,
		PreviousMemoryID: resp.PreviousMemoryID,
		ReEmbedded:       resp.ReEmbedded,
		Superseded:       resp.Superseded,
	}
}

// mcpMemoryDetail is the memory_get projection. derived_from is hoisted from
// metadata so dream lineage stays resolvable.
type mcpMemoryDetail struct {
	ID          uuid.UUID          `json:"id"`
	ProjectSlug string             `json:"project_slug"`
	Content     string             `json:"content"`
	Tags        []string           `json:"tags"`
	Source      *string            `json:"source,omitempty"`
	Origin      model.MemoryOrigin `json:"origin"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	DerivedFrom []uuid.UUID        `json:"derived_from,omitempty"`
	Metadata    json.RawMessage    `json:"metadata,omitempty"`
}

func buildMCPMemoryDetail(d service.MemoryDetail, projectSlug string, opts projectionOpts) mcpMemoryDetail {
	derived, meta := extractDerivedFrom(d.Metadata, opts)
	return mcpMemoryDetail{
		ID:          d.ID,
		ProjectSlug: projectSlug,
		Content:     d.Content,
		Tags:        d.Tags,
		Source:      d.Source,
		Origin:      d.Origin,
		CreatedAt:   d.CreatedAt,
		UpdatedAt:   d.UpdatedAt,
		DerivedFrom: derived,
		Metadata:    meta,
	}
}

// mcpBatchGetResponse is the typed response for the get MCP tool.
//
// Truncated is RESERVED for newBatchGetReducer (result_limit.go) and MUST
// NOT be set by handler code. It indicates "this response was shrunk to fit
// the MCP token budget"; handlers that set it would mislead clients into
// treating a complete result as partial.
type mcpBatchGetResponse struct {
	Found     []mcpMemoryDetail `json:"found"`
	NotFound  []uuid.UUID       `json:"not_found"`
	Truncated *truncationInfo   `json:"_truncated,omitempty"`
}

// buildMCPBatchGetResponse stamps every result with projectSlug because
// memory_get is project-scoped; BatchGet filters to a single namespace, so
// all returned memories share the request's project.
func buildMCPBatchGetResponse(resp *service.BatchGetResponse, projectSlug string, opts projectionOpts) *mcpBatchGetResponse {
	found := make([]mcpMemoryDetail, 0, len(resp.Found))
	for _, d := range resp.Found {
		found = append(found, buildMCPMemoryDetail(d, projectSlug, opts))
	}
	return &mcpBatchGetResponse{
		Found:    found,
		NotFound: resp.NotFound,
	}
}
