package mcp

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/service"
)

type mcpStoreResponse struct {
	ID               uuid.UUID `json:"id"`
	ProjectSlug      string    `json:"project_slug"`
	Enriched         bool      `json:"enriched"`
	EnrichmentQueued bool      `json:"enrichment_queued,omitempty"`
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

type mcpUpdateResponse struct {
	ID         uuid.UUID `json:"id"`
	ReEmbedded bool      `json:"re_embedded"`
}

func buildMCPUpdateResponse(resp *service.UpdateResponse) *mcpUpdateResponse {
	return &mcpUpdateResponse{
		ID:         resp.ID,
		ReEmbedded: resp.ReEmbedded,
	}
}

// mcpMemoryDetail is the memory_get projection. derived_from is hoisted from
// metadata so dream lineage stays resolvable.
type mcpMemoryDetail struct {
	ID          uuid.UUID       `json:"id"`
	ProjectSlug string          `json:"project_slug"`
	Content     string          `json:"content"`
	Tags        []string        `json:"tags"`
	Source      *string         `json:"source,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	DerivedFrom []uuid.UUID     `json:"derived_from,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}

func buildMCPMemoryDetail(d service.MemoryDetail, projectSlug string, opts projectionOpts) mcpMemoryDetail {
	derived, meta := extractDerivedFrom(d.Metadata, opts)
	return mcpMemoryDetail{
		ID:          d.ID,
		ProjectSlug: projectSlug,
		Content:     d.Content,
		Tags:        d.Tags,
		Source:      d.Source,
		CreatedAt:   d.CreatedAt,
		UpdatedAt:   d.UpdatedAt,
		DerivedFrom: derived,
		Metadata:    meta,
	}
}

type mcpBatchGetResponse struct {
	Found    []mcpMemoryDetail `json:"found"`
	NotFound []uuid.UUID       `json:"not_found"`
}

// buildMCPBatchGetResponse stamps every result with projectSlug because
// memory_get is project-scoped — BatchGet filters to a single namespace, so
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
