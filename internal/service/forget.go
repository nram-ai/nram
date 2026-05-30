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
	"github.com/nram-ai/nram/internal/observability/metrics"
	"github.com/nram-ai/nram/internal/storage"
)

// MemoryDeleter provides delete and read operations needed by the forget service.
type MemoryDeleter interface {
	SoftDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
	HardDelete(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID, limit, offset int) ([]model.Memory, error)
	// FindBySupersededBy returns the IDs of live memories whose
	// superseded_by column equals id. Used by the forget cascade to walk
	// supersede chains so forgetting the active head also forgets older
	// versions of the same memory thread.
	FindBySupersededBy(ctx context.Context, namespaceID uuid.UUID, id uuid.UUID) ([]uuid.UUID, error)
}

// VectorDeleter provides vector store deletion.
type VectorDeleter interface {
	Delete(ctx context.Context, kind storage.VectorKind, id uuid.UUID) error
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
// single ID delete, bulk delete, and filter-based delete. Hard delete relies
// on schema-level FK ON DELETE actions to clean up child rows.
type ForgetService struct {
	memories       MemoryDeleter
	projects       ProjectRepository
	vectorStore    VectorDeleter
	lineageQuerier LineageQuerier
	metrics        *metrics.Metrics
}

// NewForgetService creates a new ForgetService with the given dependencies.
func NewForgetService(
	memories MemoryDeleter,
	projects ProjectRepository,
	vectorStore VectorDeleter,
	lineageQuerier LineageQuerier,
) *ForgetService {
	return &ForgetService{
		memories:       memories,
		projects:       projects,
		vectorStore:    vectorStore,
		lineageQuerier: lineageQuerier,
	}
}

// WithMetrics attaches the Prometheus metrics sink. Returns the same service
// for chaining at construction time.
func (s *ForgetService) WithMetrics(m *metrics.Metrics) *ForgetService {
	s.metrics = m
	return s
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

	// Visited set spans the entire request so a cycle within one root cannot
	// be re-entered when a sibling root happens to land on the same node, and
	// each cascade frame is bounded regardless of the lineage shape on disk.
	visited := make(map[uuid.UUID]struct{})

	// Single memory ID delete.
	if hasMemoryID {
		n, err := s.deleteSingle(ctx, *req.MemoryID, project.NamespaceID, req.HardDelete, visited)
		if err != nil {
			return nil, err
		}
		deleted += n
	}

	// Bulk delete by IDs.
	if hasMemoryIDs {
		for _, id := range req.MemoryIDs {
			n, err := s.deleteSingle(ctx, id, project.NamespaceID, req.HardDelete, visited)
			if err != nil {
				log.Printf("forget: delete %s: %v", id, err)
				continue
			}
			deleted += n
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
					n, err := s.deleteSingle(ctx, mem.ID, project.NamespaceID, req.HardDelete, visited)
					if err != nil {
						continue
					}
					deleted += n
				}
			}

			if len(memories) < batchSize {
				break
			}
			offset += batchSize
		}
	}

	latency := time.Since(start).Milliseconds()

	if s.metrics != nil {
		s.metrics.AddMemoriesForgotten(float64(deleted))
	}

	return &ForgetResponse{
		Deleted:   deleted,
		LatencyMs: latency,
	}, nil
}

// cascadeRelations restricts the forget cascade to extraction edges. Other
// relations (model.LineageSupersedes, LineageConflictsWith, LineageSynthesizedFrom)
// can self-reference or form cycles and are not the caller's intent when
// deleting by memory ID.
var cascadeRelations = []string{
	model.LineageExtractedFrom,
	model.LineageExtractedFact,
}

// deleteSingle deletes a single memory after verifying it belongs to the given namespace.
// Returns the total number of memories deleted by this call, including any
// cascaded supersede ancestors and extraction children. Returns 0 when the
// memory is missing, already visited, or in another namespace.
// The visited set is shared across all calls for one Forget request so cyclic
// or already-handled lineage cannot loop the cascade.
func (s *ForgetService) deleteSingle(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, hard bool, visited map[uuid.UUID]struct{}) (int, error) {
	if _, ok := visited[id]; ok {
		return 0, nil
	}
	visited[id] = struct{}{}

	mem, err := s.memories.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil // genuinely not found
		}
		// Propagate real errors (SQLITE_BUSY, network, etc.) instead of
		// silently treating them as "not found".
		return 0, fmt.Errorf("forget lookup %s: %w", id, err)
	}

	// Verify memory belongs to the project's namespace.
	if mem.NamespaceID != namespaceID {
		return 0, nil
	}

	cascaded := 0

	// Cascade: walk the supersede chain and delete older versions of this
	// memory thread. Forgetting the active head forgets the whole thread —
	// brain-like, since the older versions are the same logical memory at
	// earlier points in time. The default soft-delete path needs an
	// explicit walk because the FK ON DELETE SET NULL on
	// memories.superseded_by only fires under hard delete.
	ancestorIDs, err := s.memories.FindBySupersededBy(ctx, namespaceID, id)
	if err != nil {
		log.Printf("cascade: find supersede ancestors for %s: %v", id, err)
	}
	for _, ancestorID := range ancestorIDs {
		n, err := s.deleteSingle(ctx, ancestorID, namespaceID, hard, visited)
		if err != nil {
			log.Printf("cascade: delete ancestor %s of %s: %v", ancestorID, id, err)
		}
		cascaded += n
	}

	// Cascade: delete child memories (extracted facts) before the parent.
	if s.lineageQuerier != nil {
		childIDs, err := s.lineageQuerier.FindChildIDsByRelation(ctx, namespaceID, id, cascadeRelations)
		if err != nil {
			log.Printf("cascade: find children for %s: %v", id, err)
		}
		for _, childID := range childIDs {
			n, err := s.deleteSingle(ctx, childID, namespaceID, hard, visited)
			if err != nil {
				log.Printf("cascade: delete child %s of %s: %v", childID, id, err)
			}
			cascaded += n
		}
	}

	if hard {
		if s.vectorStore != nil {
			_ = s.vectorStore.Delete(ctx, storage.VectorKindMemory, id)
		}

		if err := s.memories.HardDelete(ctx, id, namespaceID); err != nil {
			return cascaded, fmt.Errorf("hard delete failed for %s: %w", id, err)
		}
	} else {
		if err := s.memories.SoftDelete(ctx, id, namespaceID); err != nil {
			return cascaded, fmt.Errorf("soft delete failed for %s: %w", id, err)
		}
	}

	return cascaded + 1, nil
}
