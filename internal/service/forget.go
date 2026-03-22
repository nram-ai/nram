package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// RelationshipCleaner deletes relationships associated with a memory.
type RelationshipCleaner interface {
	DeleteBySourceMemory(ctx context.Context, memoryID uuid.UUID) error
}

// LineageCleaner deletes lineage records associated with a memory.
type LineageCleaner interface {
	DeleteByMemoryID(ctx context.Context, memoryID uuid.UUID) error
}

// EnrichmentQueueCleaner deletes enrichment queue items associated with a memory.
type EnrichmentQueueCleaner interface {
	DeleteByMemoryID(ctx context.Context, memoryID uuid.UUID) error
}

// TokenUsageCleaner deletes token usage records associated with a memory.
type TokenUsageCleaner interface {
	DeleteByMemoryID(ctx context.Context, memoryID uuid.UUID) error
}

// MemoryDeleter provides delete and read operations needed by the forget service.
type MemoryDeleter interface {
	SoftDelete(ctx context.Context, id uuid.UUID) error
	HardDelete(ctx context.Context, id uuid.UUID) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error)
}

// VectorDeleter provides vector store deletion.
type VectorDeleter interface {
	Delete(ctx context.Context, id uuid.UUID) error
}

// ForgetRequest contains all parameters needed to forget (delete) memories.
type ForgetRequest struct {
	ProjectID  uuid.UUID   `json:"project_id"`
	MemoryID   *uuid.UUID  `json:"memory_id,omitempty"`
	MemoryIDs  []uuid.UUID `json:"memory_ids,omitempty"`
	Tags       []string    `json:"tags,omitempty"`
	HardDelete bool        `json:"hard_delete"`
	// Caller context
	UserID *uuid.UUID `json:"-"`
	OrgID  *uuid.UUID `json:"-"`
}

// ForgetResponse contains the result of a forget operation.
type ForgetResponse struct {
	Deleted   int   `json:"deleted"`
	LatencyMs int64 `json:"latency_ms"`
}

// ForgetService orchestrates memory deletion: soft delete, hard delete,
// single ID delete, bulk delete, and filter-based delete.
type ForgetService struct {
	memories             MemoryDeleter
	projects             ProjectRepository
	vectorStore          VectorDeleter
	relationshipCleaner  RelationshipCleaner
	lineageCleaner       LineageCleaner
	enrichmentCleaner    EnrichmentQueueCleaner
	tokenUsageCleaner    TokenUsageCleaner
}

// NewForgetService creates a new ForgetService with the given dependencies.
func NewForgetService(
	memories MemoryDeleter,
	projects ProjectRepository,
	vectorStore VectorDeleter,
	relationshipCleaner RelationshipCleaner,
	lineageCleaner LineageCleaner,
	enrichmentCleaner EnrichmentQueueCleaner,
	tokenUsageCleaner TokenUsageCleaner,
) *ForgetService {
	return &ForgetService{
		memories:            memories,
		projects:            projects,
		vectorStore:         vectorStore,
		relationshipCleaner: relationshipCleaner,
		lineageCleaner:      lineageCleaner,
		enrichmentCleaner:   enrichmentCleaner,
		tokenUsageCleaner:   tokenUsageCleaner,
	}
}

// Forget deletes memories according to the request parameters.
func (s *ForgetService) Forget(ctx context.Context, req *ForgetRequest) (*ForgetResponse, error) {
	start := time.Now()

	// Validate: project_id is required.
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}

	// Validate: at least one filter must be provided.
	hasMemoryID := req.MemoryID != nil
	hasMemoryIDs := len(req.MemoryIDs) > 0
	hasTags := len(req.Tags) > 0

	if !hasMemoryID && !hasMemoryIDs && !hasTags {
		return nil, fmt.Errorf("at least one of memory_id, memory_ids, or tags must be provided")
	}

	// Validate: memory_ids must not be an empty slice if provided as non-nil.
	// (len check above already handles this — if provided but empty, hasMemoryIDs is false,
	// so this case falls through to the "no filter" error above if nothing else is set.)

	// Look up project to verify it exists.
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	var deleted int

	// Single memory ID delete.
	if hasMemoryID {
		ok, err := s.deleteSingle(ctx, *req.MemoryID, project.NamespaceID, req.HardDelete)
		if err != nil {
			return nil, err
		}
		if ok {
			deleted++
		}
	}

	// Bulk delete by IDs.
	if hasMemoryIDs {
		for _, id := range req.MemoryIDs {
			ok, err := s.deleteSingle(ctx, id, project.NamespaceID, req.HardDelete)
			if err != nil {
				log.Printf("forget: delete %s: %v", id, err)
				continue
			}
			if ok {
				deleted++
			}
		}
	}

	// Tag-based delete: list all memories in namespace, filter by tag intersection.
	if hasTags {
		const batchSize = 100
		offset := 0
		for {
			memories, err := s.memories.ListByNamespace(ctx, project.NamespaceID, batchSize, offset)
			if err != nil {
				break
			}
			if len(memories) == 0 {
				break
			}

			for _, mem := range memories {
				if hasAllTags(mem.Tags, req.Tags) {
					ok, err := s.deleteSingle(ctx, mem.ID, project.NamespaceID, req.HardDelete)
					if err != nil {
						continue
					}
					if ok {
						deleted++
					}
				}
			}

			if len(memories) < batchSize {
				break
			}
			offset += batchSize
		}
	}

	latency := time.Since(start).Milliseconds()

	return &ForgetResponse{
		Deleted:   deleted,
		LatencyMs: latency,
	}, nil
}

// deleteSingle deletes a single memory after verifying it belongs to the given namespace.
// Returns true if the memory was deleted, false if it was skipped (e.g., not found).
func (s *ForgetService) deleteSingle(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, hard bool) (bool, error) {
	mem, err := s.memories.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil // genuinely not found
		}
		// Propagate real errors (SQLITE_BUSY, network, etc.) instead of
		// silently treating them as "not found".
		return false, fmt.Errorf("forget lookup %s: %w", id, err)
	}

	// Verify memory belongs to the project's namespace.
	if mem.NamespaceID != namespaceID {
		return false, nil
	}

	if hard {
		// Remove FK references BEFORE deleting the memory to avoid
		// FOREIGN KEY constraint failures (enrichment_queue, token_usage,
		// memory_lineage, relationships all reference memories without CASCADE).
		s.cleanupRelatedData(ctx, id)
		if s.vectorStore != nil {
			_ = s.vectorStore.Delete(ctx, id)
		}

		if err := s.memories.HardDelete(ctx, id); err != nil {
			return false, fmt.Errorf("hard delete failed for %s: %w", id, err)
		}
	} else {
		if err := s.memories.SoftDelete(ctx, id); err != nil {
			return false, fmt.Errorf("soft delete failed for %s: %w", id, err)
		}
	}

	return true, nil
}

// cleanupRelatedData performs best-effort removal of data associated with a
// deleted memory. Errors are logged but do not cause the delete to fail.
func (s *ForgetService) cleanupRelatedData(ctx context.Context, memoryID uuid.UUID) {
	if s.relationshipCleaner != nil {
		if err := s.relationshipCleaner.DeleteBySourceMemory(ctx, memoryID); err != nil {
			log.Printf("cascade cleanup: relationships for memory %s: %v", memoryID, err)
		}
	}
	if s.lineageCleaner != nil {
		if err := s.lineageCleaner.DeleteByMemoryID(ctx, memoryID); err != nil {
			log.Printf("cascade cleanup: lineage for memory %s: %v", memoryID, err)
		}
	}
	if s.enrichmentCleaner != nil {
		if err := s.enrichmentCleaner.DeleteByMemoryID(ctx, memoryID); err != nil {
			log.Printf("cascade cleanup: enrichment queue for memory %s: %v", memoryID, err)
		}
	}
	if s.tokenUsageCleaner != nil {
		if err := s.tokenUsageCleaner.DeleteByMemoryID(ctx, memoryID); err != nil {
			log.Printf("cascade cleanup: token usage for memory %s: %v", memoryID, err)
		}
	}
}
