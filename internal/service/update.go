package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/tags"
)

// MemoryUpdater defines the memory persistence operations needed by the update service.
// SupersedeReplacing atomically inserts a new memory row, marks the old row
// superseded, and writes the supersedes lineage edge in one transaction.
type MemoryUpdater interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	Update(ctx context.Context, mem *model.Memory) error
	SupersedeReplacing(ctx context.Context, oldID uuid.UUID, newMem *model.Memory, lineage *model.MemoryLineage) error
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
//
// On a content change the response ID is the NEW (active) memory ID — the
// old row is superseded and reachable only via include_superseded reads.
// On a tags/metadata-only update the ID is unchanged. PreviousMemoryID
// echoes the request's MemoryID so callers correlating events or webhooks
// can map old -> new without needing to inspect the lineage table.
type UpdateResponse struct {
	ID               uuid.UUID          `json:"id"`
	PreviousMemoryID uuid.UUID          `json:"previous_memory_id"`
	ProjectID        uuid.UUID          `json:"project_id"`
	Content          string             `json:"content"`
	Tags             []string           `json:"tags"`
	PreviousContent  string             `json:"previous_content"`
	ReEmbedded       bool               `json:"re_embedded"`
	Superseded       bool               `json:"superseded"`
	Origin           model.MemoryOrigin `json:"origin"`
	LatencyMs        int64              `json:"latency_ms"`
}

// UpdateService orchestrates memory updates, re-embedding, and lineage
// tracking. token_usage recording is handled by the UsageRecordingProvider
// middleware wrapping the registry-issued embedding provider.
type UpdateService struct {
	memories        MemoryUpdater
	projects        ProjectRepository
	vectorStore     VectorStoreWriter
	embedProvider   func() provider.EmbeddingProvider
	enrichmentQueue EnrichmentQueueRepository
}

// NewUpdateService creates a new UpdateService with the given dependencies.
func NewUpdateService(
	memories MemoryUpdater,
	projects ProjectRepository,
	vectorStore VectorStoreWriter,
	embedProvider func() provider.EmbeddingProvider,
	enrichmentQueue EnrichmentQueueRepository,
) *UpdateService {
	return &UpdateService{
		memories:        memories,
		projects:        projects,
		vectorStore:     vectorStore,
		embedProvider:   embedProvider,
		enrichmentQueue: enrichmentQueue,
	}
}

// Update modifies an existing memory's content, tags, and/or metadata.
//
// Tags-only and metadata-only updates mutate the row in place; the
// response ID matches the input MemoryID.
//
// A content change splits the memory thread: a NEW memory row is created
// with the new content, the old row is marked SupersededBy = newID, and
// the old vector + entities + relationships stay frozen with the old
// content. Recall surfaces the new ID by default; old versions are
// reachable via include_superseded.
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

	if req.Tags != nil {
		normalized := tags.Normalize(*req.Tags)
		if normalized == nil {
			normalized = []string{}
		}
		req.Tags = &normalized
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

	previousContent := mem.Content
	contentChanged := req.Content != nil && *req.Content != mem.Content

	if contentChanged {
		return s.updateSupersede(ctx, req, project, mem, previousContent, start)
	}
	return s.updateInPlace(ctx, req, project, mem, previousContent, start)
}

// updateInPlace handles tags-only and metadata-only updates (and content
// updates where the new content equals the old). The memory row mutates
// in place; no chain link is created. The response ID is the input ID.
func (s *UpdateService) updateInPlace(
	ctx context.Context,
	req *UpdateRequest,
	project *model.Project,
	mem *model.Memory,
	previousContent string,
	start time.Time,
) (*UpdateResponse, error) {
	if req.Content != nil {
		mem.Content = *req.Content
	}
	if req.Tags != nil {
		mem.Tags = *req.Tags
	}
	if req.Metadata != nil {
		mem.Metadata = *req.Metadata
	}
	mem.UpdatedAt = time.Now()

	if err := s.memories.Update(ctx, mem); err != nil {
		return nil, fmt.Errorf("failed to update memory: %w", err)
	}

	memTags := mem.Tags
	if memTags == nil {
		memTags = []string{}
	}
	return &UpdateResponse{
		ID:               mem.ID,
		PreviousMemoryID: mem.ID,
		ProjectID:        project.ID,
		Content:          mem.Content,
		Tags:             memTags,
		PreviousContent:  previousContent,
		ReEmbedded:       false,
		Superseded:       false,
		Origin:           mem.Origin,
		LatencyMs:        time.Since(start).Milliseconds(),
	}, nil
}

// updateSupersede handles a content change. It creates a new memory row,
// marks the old row superseded, writes the supersedes lineage edge in one
// transaction, embeds the new content, upserts the new vector, and queues
// fresh enrichment for the new ID. The old vector, entities, and
// relationships stay attached to the old row (frozen with the old
// content) until phase_pruning sweeps superseded rows after their grace
// window.
func (s *UpdateService) updateSupersede(
	ctx context.Context,
	req *UpdateRequest,
	project *model.Project,
	mem *model.Memory,
	previousContent string,
	start time.Time,
) (*UpdateResponse, error) {
	now := time.Now().UTC()
	newID := uuid.New()

	// Inherit policy fields (Source, Origin, Importance, ExpiresAt, PurgeAfter)
	// because the logical memory is the same — only the content moved. Origin
	// in particular must survive supersession: a re-worded dream synthesis is
	// still a dream and must stay subject to the dream-recursion guard.
	// Reset access metrics (AccessCount, LastAccessed, Confidence) and
	// Enriched because the new trace has no recall history yet and needs
	// its own enrichment pass.
	newTags := mem.Tags
	if req.Tags != nil {
		newTags = *req.Tags
	}
	if newTags == nil {
		newTags = []string{}
	}
	newMetadata := mem.Metadata
	if req.Metadata != nil {
		newMetadata = *req.Metadata
	}
	newMem := &model.Memory{
		ID:           newID,
		NamespaceID:  mem.NamespaceID,
		Content:      *req.Content,
		ContentHash:  storage.HashContent(*req.Content),
		Source:       mem.Source,
		Origin:       mem.Origin,
		Tags:         newTags,
		Confidence:   1.0,
		Importance:   mem.Importance,
		AccessCount:  0,
		LastAccessed: nil,
		ExpiresAt:    mem.ExpiresAt,
		PurgeAfter:   mem.PurgeAfter,
		Enriched:     false,
		Metadata:     newMetadata,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	// Embed the new content. If the embed succeeds we set EmbeddingDim
	// and try the vector upsert; if the vector upsert fails we drop the
	// dim so the row stays honest. Failures here do not block the
	// supersede write — the embedding-backfill phase will repair on the
	// next dream cycle and the queued enrichment job runs the embed too.
	reEmbedded := false
	var newEmbedding []float32
	var newEmbeddingDim int
	if s.embedProvider != nil {
		ep := s.embedProvider()
		if ep != nil {
			dim := bestEmbeddingDimension(ep.Dimensions())
			projectIDForCtx := project.ID
			embCtx := provider.WithUsageContext(ctx, &model.UsageContext{
				OrgID:     req.OrgID,
				UserID:    req.UserID,
				ProjectID: &projectIDForCtx,
			})
			embCtx = provider.WithNamespaceID(embCtx, mem.NamespaceID)
			embCtx = provider.WithMemoryID(embCtx, newID)
			embCtx = provider.WithAPIKeyID(embCtx, req.APIKeyID)
			embCtx = provider.WithOperation(embCtx, provider.OperationEmbedding)

			resp, embErr := ep.Embed(embCtx, &provider.EmbeddingRequest{
				Input:     []string{newMem.Content},
				Dimension: dim,
			})
			if embErr == nil && len(resp.Embeddings) > 0 {
				newEmbedding = resp.Embeddings[0]
				newEmbeddingDim = len(newEmbedding)
				newMem.EmbeddingDim = &newEmbeddingDim
			} else if embErr != nil {
				slog.Warn("memory update: embed failed; supersede proceeds without vector",
					"old_memory", mem.ID, "new_memory", newID, "err", embErr)
			}
		}
	}

	lineage := &model.MemoryLineage{
		NamespaceID: mem.NamespaceID,
		ParentID:    &mem.ID,
		Relation:    model.LineageSupersedes,
		Context:     json.RawMessage(fmt.Sprintf(`{"previous_content":%q}`, previousContent)),
	}

	if err := s.memories.SupersedeReplacing(ctx, mem.ID, newMem, lineage); err != nil {
		return nil, fmt.Errorf("failed to supersede memory: %w", err)
	}

	// Best-effort vector upsert at the new ID. The transaction has
	// already committed; if upsert fails we patch the row to drop the
	// dim claim so the backfill phase picks it up.
	if newEmbedding != nil && s.vectorStore != nil {
		if err := s.vectorStore.Upsert(ctx, storage.VectorKindMemory, newID, mem.NamespaceID, newEmbedding, newEmbeddingDim); err != nil {
			slog.Warn("memory update: vector upsert at new ID failed; clearing embedding_dim",
				"new_memory", newID, "dim", newEmbeddingDim, "err", err)
			newMem.EmbeddingDim = nil
			if uerr := s.memories.Update(ctx, newMem); uerr != nil {
				slog.Warn("memory update: clearing embedding_dim failed",
					"new_memory", newID, "err", uerr)
			}
		} else {
			reEmbedded = true
		}
	}

	// Best-effort: enqueue enrichment for the new ID so entities and
	// relationships rebuild against the new content. The old row keeps
	// its enrichment intact.
	if s.enrichmentQueue != nil {
		job := &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    newID,
			NamespaceID: mem.NamespaceID,
			Status:      "pending",
			Priority:    0,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}
		if _, err := s.enrichmentQueue.Enqueue(ctx, job); err != nil {
			slog.Warn("memory update: enrichment enqueue failed; relying on dream backfill",
				"new_memory", newID, "err", err)
		}
	}

	latency := time.Since(start).Milliseconds()
	memTags := newMem.Tags
	if memTags == nil {
		memTags = []string{}
	}
	return &UpdateResponse{
		ID:               newID,
		PreviousMemoryID: mem.ID,
		ProjectID:        project.ID,
		Content:          newMem.Content,
		Tags:             memTags,
		PreviousContent:  previousContent,
		ReEmbedded:       reEmbedded,
		Superseded:       true,
		Origin:           newMem.Origin,
		LatencyMs:        latency,
	}, nil
}
