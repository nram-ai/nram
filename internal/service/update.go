package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// MemoryUpdater defines the memory persistence operations needed by the update service.
type MemoryUpdater interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	Update(ctx context.Context, mem *model.Memory) error
}

// LineageCreator defines the lineage persistence operations needed by the update service.
type LineageCreator interface {
	Create(ctx context.Context, lineage *model.MemoryLineage) error
}

// UpdateRequest contains all parameters needed to update an existing memory.
type UpdateRequest struct {
	ProjectID uuid.UUID        `json:"project_id"`
	MemoryID  uuid.UUID        `json:"memory_id"`
	Content   *string          `json:"content,omitempty"`
	Tags      *[]string        `json:"tags,omitempty"`
	Metadata  *json.RawMessage `json:"metadata,omitempty"`
	// Caller context (set by handler/middleware)
	UserID   *uuid.UUID `json:"-"`
	OrgID    *uuid.UUID `json:"-"`
	APIKeyID *uuid.UUID `json:"-"`
}

// UpdateResponse contains the result of a memory update operation.
type UpdateResponse struct {
	ID              uuid.UUID `json:"id"`
	ProjectID       uuid.UUID `json:"project_id"`
	Content         string    `json:"content"`
	Tags            []string  `json:"tags"`
	PreviousContent string    `json:"previous_content"`
	ReEmbedded      bool      `json:"re_embedded"`
	LatencyMs       int64     `json:"latency_ms"`
}

// UpdateService orchestrates memory updates, re-embedding, lineage tracking,
// and token usage recording.
type UpdateService struct {
	memories      MemoryUpdater
	projects      ProjectRepository
	lineage       LineageCreator
	vectorStore   VectorStoreWriter
	tokenUsage    TokenUsageRepository
	embedProvider func() provider.EmbeddingProvider
}

// NewUpdateService creates a new UpdateService with the given dependencies.
func NewUpdateService(
	memories MemoryUpdater,
	projects ProjectRepository,
	lineage LineageCreator,
	vectorStore VectorStoreWriter,
	tokenUsage TokenUsageRepository,
	embedProvider func() provider.EmbeddingProvider,
) *UpdateService {
	return &UpdateService{
		memories:      memories,
		projects:      projects,
		lineage:       lineage,
		vectorStore:   vectorStore,
		tokenUsage:    tokenUsage,
		embedProvider: embedProvider,
	}
}

// Update modifies an existing memory's content, tags, and/or metadata.
// If the content changes and an embedding provider is available, the memory
// is re-embedded and a "supersedes" lineage record is created.
func (s *UpdateService) Update(ctx context.Context, req *UpdateRequest) (*UpdateResponse, error) {
	start := time.Now()

	// Validate required fields.
	if req.MemoryID == uuid.Nil {
		return nil, fmt.Errorf("memory_id is required")
	}
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}
	if req.Content == nil && req.Tags == nil && req.Metadata == nil {
		return nil, fmt.Errorf("at least one of content, tags, or metadata must be provided")
	}

	// Look up project.
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	// Get existing memory.
	mem, err := s.memories.GetByID(ctx, req.MemoryID)
	if err != nil {
		return nil, fmt.Errorf("memory not found: %w", err)
	}

	// Verify memory belongs to the project's namespace.
	if mem.NamespaceID != project.NamespaceID {
		return nil, fmt.Errorf("memory does not belong to project namespace")
	}

	// Reject edits to a paraphrase-dedup or contradiction loser. Updating the
	// loser would silently diverge from the winner the rest of the system has
	// already pointed callers at; surface the winner so the caller can retry.
	if mem.SupersededBy != nil {
		return nil, fmt.Errorf("memory %s is superseded by %s; update that memory instead", mem.ID, *mem.SupersededBy)
	}

	// Store previous content for the response.
	previousContent := mem.Content

	// Track whether content changed for re-embedding and lineage.
	contentChanged := false

	// Apply updates.
	if req.Content != nil && *req.Content != mem.Content {
		mem.Content = *req.Content
		contentChanged = true
	}
	if req.Tags != nil {
		mem.Tags = *req.Tags
	}
	if req.Metadata != nil {
		mem.Metadata = *req.Metadata
	}

	mem.UpdatedAt = time.Now()

	// Re-embed if content changed and provider is available.
	reEmbedded := false
	if contentChanged && s.embedProvider != nil {
		ep := s.embedProvider()
		if ep != nil {
			dim := bestEmbeddingDimension(ep.Dimensions())

			embReq := &provider.EmbeddingRequest{
				Input:     []string{mem.Content},
				Dimension: dim,
			}

			resp, embErr := ep.Embed(ctx, embReq)
			if embErr == nil && len(resp.Embeddings) > 0 {
				reEmbedded = true
				embDim := len(resp.Embeddings[0])
				mem.EmbeddingDim = &embDim

				if s.vectorStore != nil {
					_ = s.vectorStore.Upsert(ctx, storage.VectorKindMemory, mem.ID, mem.NamespaceID, resp.Embeddings[0], embDim)
				}

				// Record token usage.
				projectID := project.ID
				usage := &model.TokenUsage{
					ID:           uuid.New(),
					OrgID:        req.OrgID,
					UserID:       req.UserID,
					ProjectID:    &projectID,
					NamespaceID:  mem.NamespaceID,
					Operation:    "embedding",
					Provider:     ep.Name(),
					Model:        resp.Model,
					TokensInput:  resp.Usage.PromptTokens,
					TokensOutput: resp.Usage.CompletionTokens,
					MemoryID:     &mem.ID,
					APIKeyID:     req.APIKeyID,
					CreatedAt:    time.Now(),
				}
				_ = s.tokenUsage.Record(ctx, usage)
			}
		}
	}

	// Create supersedes lineage record if content changed.
	if contentChanged {
		lineageRecord := &model.MemoryLineage{
			ID:          uuid.New(),
			NamespaceID: mem.NamespaceID,
			MemoryID:    mem.ID,
			ParentID:    &mem.ID,
			Relation:    model.LineageSupersedes,
			Context:     json.RawMessage(fmt.Sprintf(`{"previous_content":%q}`, previousContent)),
			CreatedAt:   time.Now(),
		}
		_ = s.lineage.Create(ctx, lineageRecord)
	}

	// Persist the updated memory.
	if err := s.memories.Update(ctx, mem); err != nil {
		return nil, fmt.Errorf("failed to update memory: %w", err)
	}

	latency := time.Since(start).Milliseconds()

	tags := mem.Tags
	if tags == nil {
		tags = []string{}
	}

	return &UpdateResponse{
		ID:              mem.ID,
		ProjectID:       project.ID,
		Content:         mem.Content,
		Tags:            tags,
		PreviousContent: previousContent,
		ReEmbedded:      reEmbedded,
		LatencyMs:       latency,
	}, nil
}
