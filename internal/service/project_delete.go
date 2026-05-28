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

// EntityBulkDeleter deletes all entities (and their cascaded aliases /
// relationships / entity_vectors) in a namespace and returns the IDs of the
// deleted rows. The IDs are used post-commit for Qdrant vector cleanup.
type EntityBulkDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) ([]uuid.UUID, error)
}

// RelationshipBulkDeleter deletes all relationships in a namespace.
type RelationshipBulkDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// TokenUsageReassigner reassigns token usage records from one project to
// another and from one namespace to another. Both methods preserve the
// per-row audit history by repointing rather than deleting.
type TokenUsageReassigner interface {
	ReassignProjectTx(ctx context.Context, tx *sql.Tx, fromProjectID, toProjectID uuid.UUID, toNamespaceID uuid.UUID) error
	ReassignNamespaceTx(ctx context.Context, tx *sql.Tx, fromNamespaceID, toProjectID, toNamespaceID uuid.UUID) error
}

// MemoryLineageDeleter deletes all memory_lineage rows for a namespace.
// Required because memory_lineage.namespace_id has no ON DELETE action.
type MemoryLineageDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// EntityAliasDeleter deletes all entity_aliases rows for a namespace.
// Required because entity_aliases.namespace_id has no ON DELETE action.
type EntityAliasDeleter interface {
	DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error
}

// IngestionLogDeleter deletes all ingestion log entries for a namespace.
type IngestionLogDeleter interface {
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

// ShareTokenSweeper revokes any share-tokens that have been orphaned (zero
// grants) as a side effect of project deletion. Optional dependency on
// ProjectDeleteService — when nil, the share-revoke sweep is skipped and
// the share-token middleware's per-request zero-grants check still gates
// access; the sweep just keeps the owner's UI consistent.
type ShareTokenSweeper interface {
	SweepZeroGrantShares(ctx context.Context, ownerUserID uuid.UUID) (int, error)
}

// ProjectOwnerLookup resolves the user who owns a given owner_namespace_id.
// Required by the share-token sweep — the share belongs to a user, not a
// namespace, so we need the user record to scope the sweep query. Satisfied
// directly by *storage.UserRepo.
type ProjectOwnerLookup interface {
	GetByNamespaceID(ctx context.Context, namespaceID uuid.UUID) (*model.User, error)
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
// memory_lineage, then memories (enrichment_queue cascades; relationships.source_memory
// and token_usage.memory_id are SET NULL), then relationships, then entity_aliases,
// then entities, then the namespace-scoped side tables (ingestion log,
// enrichment queue, HNSW snapshots), then token_usage reassignment by
// project_id and by namespace_id, then the project row, then the namespace row.
// Either the whole cascade succeeds or the transaction rolls back and the
// database is unchanged. Vector store cleanup (in-memory HNSW graph nodes)
// and event emission run after commit.
type ProjectDeleteService struct {
	txBeginner           TxBeginner
	projectGetter        ProjectDeleteGetter
	projectDeleter       ProjectDeleter
	memoryIDLister       MemoryIDLister
	memoryLineageDeleter MemoryLineageDeleter
	memoryBulkDeleter    MemoryBulkDeleter
	vectorStore          VectorDeleter
	entityAliasDeleter   EntityAliasDeleter
	entityDeleter        EntityBulkDeleter
	relationshipDeleter  RelationshipBulkDeleter
	enrichmentDeleter    EnrichmentBulkDeleter
	tokenUsageReassign   TokenUsageReassigner
	ingestionDeleter     IngestionLogDeleter
	hnswDeleter          HNSWSnapshotDeleter
	namespaceDeleter     NamespaceDeleter
	eventBus             events.EventBus
	shareSweeper         ShareTokenSweeper  // optional
	projectOwnerLookup   ProjectOwnerLookup // optional, paired with shareSweeper
}

// NewProjectDeleteService creates a new ProjectDeleteService with the given dependencies.
func NewProjectDeleteService(
	txBeginner TxBeginner,
	projectGetter ProjectDeleteGetter,
	projectDeleter ProjectDeleter,
	memoryIDLister MemoryIDLister,
	memoryLineageDeleter MemoryLineageDeleter,
	memoryBulkDeleter MemoryBulkDeleter,
	vectorStore VectorDeleter,
	entityAliasDeleter EntityAliasDeleter,
	entityDeleter EntityBulkDeleter,
	relationshipDeleter RelationshipBulkDeleter,
	enrichmentDeleter EnrichmentBulkDeleter,
	tokenUsageReassign TokenUsageReassigner,
	ingestionDeleter IngestionLogDeleter,
	hnswDeleter HNSWSnapshotDeleter,
	namespaceDeleter NamespaceDeleter,
	eventBus events.EventBus,
) *ProjectDeleteService {
	return &ProjectDeleteService{
		txBeginner:           txBeginner,
		projectGetter:        projectGetter,
		projectDeleter:       projectDeleter,
		memoryIDLister:       memoryIDLister,
		memoryLineageDeleter: memoryLineageDeleter,
		memoryBulkDeleter:    memoryBulkDeleter,
		vectorStore:          vectorStore,
		entityAliasDeleter:   entityAliasDeleter,
		entityDeleter:        entityDeleter,
		relationshipDeleter:  relationshipDeleter,
		enrichmentDeleter:    enrichmentDeleter,
		tokenUsageReassign:   tokenUsageReassign,
		ingestionDeleter:     ingestionDeleter,
		hnswDeleter:          hnswDeleter,
		namespaceDeleter:     namespaceDeleter,
		eventBus:             eventBus,
	}
}

// WithShareSweeper wires the optional share-token sweep that runs post-
// commit on project deletion. When configured, any share owned by the
// project's owner that ends up with zero grants (because the FK cascade
// dropped its last grant row) is auto-revoked, matching the cascade rule
// in the share-token design.
func (s *ProjectDeleteService) WithShareSweeper(sweeper ShareTokenSweeper, ownerLookup ProjectOwnerLookup) *ProjectDeleteService {
	s.shareSweeper = sweeper
	s.projectOwnerLookup = ownerLookup
	return s
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

	entityIDs, err := s.runCascadeTx(ctx, project, globalProject)
	if err != nil {
		return nil, err
	}

	// Post-commit best-effort cleanup of in-process state. Persistence is
	// already correct; failures here only delay reclaim of in-memory resources.
	// SQL-backed vector stores cascade on the entity row delete; Qdrant does
	// not, so the per-entity Delete keeps the vector collections in sync.
	if s.vectorStore != nil {
		for _, memID := range memoryIDs {
			if err := s.vectorStore.Delete(ctx, storage.VectorKindMemory, memID); err != nil {
				log.Printf("project delete: vector for memory %s: %v", memID, err)
			}
		}
		for _, entID := range entityIDs {
			if err := s.vectorStore.Delete(ctx, storage.VectorKindEntity, entID); err != nil {
				log.Printf("project delete: vector for entity %s: %v", entID, err)
			}
		}
	}

	// Sweep share-tokens that lost their last grant via FK cascade. The
	// middleware-level zero-grants gate already blocks runtime access from
	// such shares; this revoke keeps the owner's UI honest by flipping the
	// status from "active" to "revoked" and triggers refresh-token cleanup.
	// Log every skip path so a missing sweep is diagnosable rather than
	// silent — the owner's UI showing "active but unusable" shares is a
	// confusing failure mode we want to alert on.
	if s.shareSweeper != nil && s.projectOwnerLookup != nil {
		owner, err := s.projectOwnerLookup.GetByNamespaceID(ctx, project.OwnerNamespaceID)
		switch {
		case err != nil:
			log.Printf("project delete: share sweep skipped, owner lookup for namespace %s failed: %v", project.OwnerNamespaceID, err)
		case owner == nil:
			log.Printf("project delete: share sweep skipped, no owner for namespace %s", project.OwnerNamespaceID)
		default:
			if n, sweepErr := s.shareSweeper.SweepZeroGrantShares(ctx, owner.ID); sweepErr != nil {
				log.Printf("project delete: share sweep: %v", sweepErr)
			} else if n > 0 {
				log.Printf("project delete: revoked %d zero-grant share(s) for owner %s", n, owner.ID)
			}
		}
	}

	if s.eventBus != nil {
		data, _ := json.Marshal(map[string]any{
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
// fully or not at all. Returns the IDs of deleted entities for post-commit
// vector cleanup.
func (s *ProjectDeleteService) runCascadeTx(ctx context.Context, project, globalProject *model.Project) ([]uuid.UUID, error) {
	tx, err := s.txBeginner.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("project delete: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// memory_lineage before memories. memory_lineage.namespace_id has no
	// ON DELETE action and the row's namespace_id is not schema-guaranteed
	// to match its parent memory's, so explicit per-namespace clearing is
	// needed before the namespace row is deleted later in the cascade.
	if err := s.memoryLineageDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: memory lineage: %w", err)
	}

	// Memories next. enrichment_queue cascades by the schema;
	// relationships.source_memory and token_usage.memory_id are SET NULL.
	if err := s.memoryBulkDeleter.HardDeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: memories: %w", err)
	}

	// Relationships before entities. Schema 000032 / 000035 added ON DELETE
	// CASCADE to relationships.{source_id,target_id} → entities, so the order
	// is no longer a correctness requirement, but explicit deletion keeps the
	// per-step errors attributable and avoids a single multi-row cascade
	// expanding under FK enforcement.
	if err := s.relationshipDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: relationships: %w", err)
	}

	// entity_aliases before entities. The entity_id FK cascades aliases when
	// their parent entity is deleted, but entity_aliases.namespace_id has no
	// ON DELETE action and is not schema-guaranteed to match its parent
	// entity's namespace_id. Explicit per-namespace clearing closes the gap.
	if err := s.entityAliasDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: entity aliases: %w", err)
	}

	// Entities. entity_vectors_* (SQL-backed stores) cascade. Returned IDs
	// feed the Qdrant vector cleanup that runs after the tx commits.
	entityIDs, err := s.entityDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("project delete: entities: %w", err)
	}

	if err := s.ingestionDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: ingestion log: %w", err)
	}

	if err := s.enrichmentDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: enrichment queue: %w", err)
	}

	// HNSW snapshots reference namespaces(id) with no ON DELETE action; clear
	// them before the namespace row is deleted below.
	if s.hnswDeleter != nil {
		if err := s.hnswDeleter.DeleteByNamespaceTx(ctx, tx, project.NamespaceID); err != nil {
			return nil, fmt.Errorf("project delete: hnsw snapshots: %w", err)
		}
	}

	// Reassign token_usage to the global project. Must precede the project
	// row delete since token_usage.project_id has no ON DELETE action. This
	// covers rows whose project_id matches the deleted project.
	if err := s.tokenUsageReassign.ReassignProjectTx(ctx, tx, project.ID, globalProject.ID, globalProject.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: token usage reassign: %w", err)
	}

	// Then catch any remaining rows still pointing at the deleted namespace
	// via namespace_id (project_id NULL or pointing at a different project).
	// token_usage.namespace_id is NOT NULL with no ON DELETE action, so the
	// namespace row delete below would otherwise fail.
	if err := s.tokenUsageReassign.ReassignNamespaceTx(ctx, tx, project.NamespaceID, globalProject.ID, globalProject.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: token usage reassign namespace: %w", err)
	}

	// Project row. Dream tables CASCADE on this delete.
	if err := s.projectDeleter.DeleteTx(ctx, tx, project.ID); err != nil {
		return nil, fmt.Errorf("project delete: project: %w", err)
	}

	if err := s.namespaceDeleter.DeleteTx(ctx, tx, project.NamespaceID); err != nil {
		return nil, fmt.Errorf("project delete: namespace: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("project delete: commit: %w", err)
	}
	committed = true
	return entityIDs, nil
}
