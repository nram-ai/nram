package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// EnrichRequest contains the parameters for an enrich operation.
type EnrichRequest struct {
	ProjectID uuid.UUID   `json:"project_id"`
	MemoryIDs []uuid.UUID `json:"memory_ids,omitempty"` // specific IDs
	All       bool        `json:"all,omitempty"`         // enrich all un-enriched
	Priority  int         `json:"priority,omitempty"`    // default 0
}

// EnrichResponse contains the result of an enrich operation.
type EnrichResponse struct {
	Queued    int   `json:"queued"`
	Skipped   int   `json:"skipped"`    // already enriched
	LatencyMs int64 `json:"latency_ms"`
}

// LineageQuerier provides read-only lineage lookups used by multiple services.
type LineageQuerier interface {
	FindParentIDs(ctx context.Context, memoryIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error)
	FindChildIDs(ctx context.Context, parentID uuid.UUID) ([]uuid.UUID, error)
}

// EnrichService orchestrates bulk enrichment queueing for memories in a project.
type EnrichService struct {
	memories        MemoryReader
	projects        ProjectRepository
	enrichmentQueue EnrichmentQueueRepository
	lineage         LineageQuerier
}

// NewEnrichService creates a new EnrichService with the given dependencies.
func NewEnrichService(
	memories MemoryReader,
	projects ProjectRepository,
	enrichmentQueue EnrichmentQueueRepository,
	lineage LineageQuerier,
) *EnrichService {
	return &EnrichService{
		memories:        memories,
		projects:        projects,
		enrichmentQueue: enrichmentQueue,
		lineage:         lineage,
	}
}

// Enrich enqueues enrichment jobs for the specified memories or all un-enriched
// memories in the project's namespace.
func (s *EnrichService) Enrich(ctx context.Context, req *EnrichRequest) (*EnrichResponse, error) {
	start := time.Now()

	// Validate required fields.
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}
	if len(req.MemoryIDs) == 0 && !req.All {
		return nil, fmt.Errorf("at least one of memory_ids or all must be specified")
	}

	// Look up project.
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	namespaceID := project.NamespaceID

	var memories []model.Memory

	if len(req.MemoryIDs) > 0 {
		// Fetch specific memories.
		batch, err := s.memories.GetBatch(ctx, req.MemoryIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch memories: %w", err)
		}
		// Filter to only memories in the project's namespace.
		for _, mem := range batch {
			if mem.NamespaceID == namespaceID {
				memories = append(memories, mem)
			}
		}
	} else {
		// Paginate through all memories in the namespace.
		const pageSize = 100
		offset := 0
		for {
			page, err := s.memories.ListByNamespace(ctx, namespaceID, pageSize, offset)
			if err != nil {
				return nil, fmt.Errorf("failed to list memories: %w", err)
			}
			memories = append(memories, page...)
			if len(page) < pageSize {
				break
			}
			offset += pageSize
		}
	}

	// Batch-lookup which memories are children to skip them.
	childSet := make(map[uuid.UUID]bool)
	if s.lineage != nil {
		ids := make([]uuid.UUID, len(memories))
		for i := range memories {
			ids[i] = memories[i].ID
		}
		if parentMap, err := s.lineage.FindParentIDs(ctx, ids); err == nil {
			for childID := range parentMap {
				childSet[childID] = true
			}
		}
	}

	// Enqueue un-enriched, non-child memories.
	queued := 0
	skipped := 0
	now := time.Now()

	for i := range memories {
		mem := &memories[i]
		if mem.Enriched || childSet[mem.ID] {
			skipped++
			continue
		}

		job := &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    mem.ID,
			NamespaceID: namespaceID,
			Status:      "pending",
			Priority:    req.Priority,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		if err := s.enrichmentQueue.Enqueue(ctx, job); err != nil {
			return nil, fmt.Errorf("failed to enqueue enrichment job for memory %s: %w", mem.ID, err)
		}
		queued++
	}

	latency := time.Since(start).Milliseconds()

	return &EnrichResponse{
		Queued:    queued,
		Skipped:   skipped,
		LatencyMs: latency,
	}, nil
}
