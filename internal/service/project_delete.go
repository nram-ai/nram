package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// ProjectDeleteGetter provides project lookup operations for the delete service.
type ProjectDeleteGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
	GetBySlug(ctx context.Context, ownerNamespaceID uuid.UUID, slug string) (*model.Project, error)
}

// TxBeginner starts a write transaction. The whole project-delete cascade
// runs inside one transaction so partial state is impossible.
type TxBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// ProjectDeleter provides project deletion.
type ProjectDeleter interface {
	DeleteTx(ctx context.Context, tx *sql.Tx, id uuid.UUID) error
}

// MemoryIDLister lists all non-deleted memory IDs in a namespace.
type MemoryIDLister interface {
	ListIDsByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]uuid.UUID, error)
}

// MemoryBulkDeleter hard-deletes all memories in a namespace.
type MemoryBulkDeleter interface {
	HardDeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// EntityBulkDeleter deletes all entities (and aliases) in a namespace.
type EntityBulkDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// RelationshipBulkDeleter deletes all relationships in a namespace.
type RelationshipBulkDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// TokenUsageReassigner reassigns token usage records from one project to another.
type TokenUsageReassigner interface {
	ReassignProjectTx(ctx context.Context, tx *sql.Tx, fromProjectID, toProjectID uuid.UUID, toNamespaceID uuid.UUID) error
}

// IngestionLogDeleter deletes all ingestion log entries for a namespace.
type IngestionLogDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// MemoryShareDeleter deletes all memory shares involving a namespace.
type MemoryShareDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// EnrichmentBulkDeleter deletes enrichment queue entries by namespace.
type EnrichmentBulkDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// HNSWSnapshotDeleter deletes HNSW snapshots by namespace and evicts the
// in-memory cache for that namespace.
type HNSWSnapshotDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// NamespaceDeleter deletes a namespace.
type NamespaceDeleter interface {
	DeleteTx(ctx context.Context, tx *sql.Tx, id uuid.UUID) error
}

// ProjectDeleteRequest contains all parameters needed to delete a project.
type ProjectDeleteRequest struct {
	ProjectID uuid.UUID `json:"project_id"`
}

// ProjectDeleteResponse contains the result of a project deletion.
type ProjectDeleteResponse struct {
	DeletedMemories int    `json:"deleted_memories"`
	ProjectSlug     string `json:"project"`
}

// ErrNoGlobalProject is returned when the project's owner has no "global"
// project to receive reassigned token_usage rows. The cascade refuses to
// delete the project rather than orphan billing records or strand them on a
// dangling project_id (the FK on token_usage.project_id has no ON DELETE
// action and would block the project row delete anyway).
var ErrNoGlobalProject = errors.New("project delete: owner has no global project to receive reassigned token usage")

// ProjectDeleteService orchestrates recursive deletion of a project and all
// associated data. Project deletion is strictly self-service: only the project
// owner can delete their own projects.
//
// The full DB cascade runs inside a single transaction, in FK-safe order:
// memories first (memory_lineage / enrichment_queue cascade, relationships.source_memory
// and token_usage.memory_id are SET NULL), then relationships (which reference
// entities), then entities, then the namespace-scoped side tables, then HNSW
// snapshots, then token_usage reassignment, then the project row, then the
// namespace row. Either the whole cascade succeeds or the transaction rolls
// back and the database is unchanged. Vector store cleanup (in-memory HNSW
// graph nodes) and event emission run after commit.
type ProjectDeleteService struct {
	txBeginner          TxBeginner
	projectGetter       ProjectDeleteGetter
	projectDeleter      ProjectDeleter
	memoryIDLister      MemoryIDLister
	memoryBulkDeleter   MemoryBulkDeleter
	vectorStore         VectorDeleter
	entityDeleter       EntityBulkDeleter
	relationshipDeleter RelationshipBulkDeleter
	enrichmentDeleter   EnrichmentBulkDeleter
	tokenUsageReassign  TokenUsageReassigner
	ingestionDeleter    IngestionLogDeleter
	shareDeleter        MemoryShareDeleter
	hnswDeleter         HNSWSnapshotDeleter
	namespaceDeleter    NamespaceDeleter
	eventBus            events.EventBus
}

// NewProjectDeleteService creates a new ProjectDeleteService with the given dependencies.
func NewProjectDeleteService(
	txBeginner TxBeginner,
	projectGetter ProjectDeleteGetter,
	projectDeleter ProjectDeleter,
	memoryIDLister MemoryIDLister,
	memoryBulkDeleter MemoryBulkDeleter,
	vectorStore VectorDeleter,
	entityDeleter EntityBulkDeleter,
	relationshipDeleter RelationshipBulkDeleter,
	enrichmentDeleter EnrichmentBulkDeleter,
	tokenUsageReassign TokenUsageReassigner,
	ingestionDeleter IngestionLogDeleter,
	shareDeleter MemoryShareDeleter,
	hnswDeleter HNSWSnapshotDeleter,
	namespaceDeleter NamespaceDeleter,
	eventBus events.EventBus,
) *ProjectDeleteService {
	return &ProjectDeleteService{
		txBeginner:          txBeginner,
		projectGetter:       projectGetter,
		projectDeleter:      projectDeleter,
		memoryIDLister:      memoryIDLister,
		memoryBulkDeleter:   memoryBulkDeleter,
		vectorStore:         vectorStore,
		entityDeleter:       entityDeleter,
		relationshipDeleter: relationshipDeleter,
		enrichmentDeleter:   enrichmentDeleter,
		tokenUsageReassign:  tokenUsageReassign,
		ingestionDeleter:    ingestionDeleter,
		shareDeleter:        shareDeleter,
		hnswDeleter:         hnswDeleter,
		namespaceDeleter:    namespaceDeleter,
		eventBus:            eventBus,
	}
}

// Delete recursively deletes a project and all associated data. The project's
// slug must not be "global". Token usage records are reassigned to the owner's
// global project rather than deleted; if no global project exists, the
// operation fails with ErrNoGlobalProject before any rows are touched.
func (s *ProjectDeleteService) Delete(ctx context.Context, req *ProjectDeleteRequest) (*ProjectDeleteResponse, error) {
	start := time.Now()

	project, err := s.projectGetter.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}
	if project.Slug == "global" {
		return nil, fmt.Errorf("the global project cannot be deleted")
	}

	// Resolve the owner's global project up front. token_usage.project_id has
	// no ON DELETE action, so we must redirect those rows or the project row
	// delete inside the cascade tx will fail with a FK violation. Refusing
	// here is preferable to silently abandoning billing rows.
	globalProject, err := s.projectGetter.GetBySlug(ctx, project.OwnerNamespaceID, "global")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNoGlobalProject, err)
	}

	// Snapshot memory IDs before the tx mutates anything. Used post-commit
	// for in-memory HNSW node cleanup and for the response payload.
	var memoryIDs []uuid.UUID
	if s.memoryIDLister != nil {
		memoryIDs, err = s.memoryIDLister.ListIDsByNamespace(ctx, project.NamespaceID)
		if err != nil {
			return nil, fmt.Errorf("list memory ids: %w", err)
		}
	}

	if err := s.runCascadeTx(ctx, project, globalProject); err != nil {
		return nil, err
	}

	// Post-commit best-effort cleanup of in-process state. Persistence is
	// already correct; failures here only delay reclaim of in-memory resources.
	if s.vectorStore != nil {
		for _, memID := range memoryIDs {
			if err := s.vectorStore.Delete(ctx, storage.VectorKindMemory, memID); err != nil {
				log.Printf("project delete: vector for memory %s: %v", memID, err)
			}
		}
	}

	if s.eventBus != nil {
		data, _ := json.Marshal(map[string]interface{}{
			"project_id":   project.ID.String(),
			"project_slug": project.Slug,
			"memories":     len(memoryIDs),
		})
		events.Emit(ctx, s.eventBus, events.ProjectDeleted, "project:"+project.ID.String(), json.RawMessage(data))
	}

	log.Printf("project delete: %s (%s) completed in %v, %d memories removed",
		project.Slug, project.ID, time.Since(start), len(memoryIDs))

	return &ProjectDeleteResponse{
		DeletedMemories: len(memoryIDs),
		ProjectSlug:     project.Slug,
	}, nil
}

// runCascadeTx executes the FK-ordered cascade inside a single transaction.
// Any step's failure rolls the whole thing back so the project either deletes
// fully or not at all.
func (s *ProjectDeleteService) runCascadeTx(ctx context.Context, project, globalProject *model.Project) error {
	tx, err := s.txBeginner.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("project delete: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Memories first: their FK children (memory_lineage, enrichment_queue) are
	// CASCADEd by the schema; relationships.source_memory and token_usage.memory_id
	// are SET NULL.
	if err := s.memoryBulkDeleter.HardDeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return fmt.Errorf("project delete: memories: %w", err)
	}

	// Relationships before entities: relationships.source_id and target_id
	// REFERENCE entities(id) with no ON DELETE action. Deleting entities first
	// raises a FK violation (this was the bug that left "memories deleted but
	// entities still there" after one click).
	if err := s.relationshipDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return fmt.Errorf("project delete: relationships: %w", err)
	}

	// Entities (the repo deletes entity_aliases inside the same call;
	// entity_vectors cascade on the entity row delete).
	if err := s.entityDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return fmt.Errorf("project delete: entities: %w", err)
	}

	if err := s.ingestionDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return fmt.Errorf("project delete: ingestion log: %w", err)
	}

	if err := s.shareDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return fmt.Errorf("project delete: memory shares: %w", err)
	}

	if err := s.enrichmentDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return fmt.Errorf("project delete: enrichment queue: %w", err)
	}

	// HNSW snapshots reference namespaces(id) with no ON DELETE action; clear
	// them before the namespace row is deleted below.
	if s.hnswDeleter != nil {
		if err := s.hnswDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
			return fmt.Errorf("project delete: hnsw snapshots: %w", err)
		}
	}

	// Reassign token_usage to the global project. Must precede the project
	// row delete since token_usage.project_id has no ON DELETE action.
	if err := s.tokenUsageReassign.ReassignProjectTx(ctx, tx, project.ID, globalProject.ID, globalProject.NamespaceID); err != nil {
		return fmt.Errorf("project delete: token usage reassign: %w", err)
	}

	// Project row. Dream tables CASCADE on this delete.
	if err := s.projectDeleter.DeleteTx(ctx, tx, project.ID); err != nil {
		return fmt.Errorf("project delete: project: %w", err)
	}

	if err := s.namespaceDeleter.DeleteTx(ctx, tx, project.NamespaceID); err != nil {
		return fmt.Errorf("project delete: namespace: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("project delete: commit: %w", err)
	}
	committed = true
	return nil
}
