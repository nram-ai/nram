package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
)

// ProjectDeleteGetter provides project lookup operations for the delete service.
type ProjectDeleteGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
	GetBySlug(ctx context.Context, ownerNamespaceID uuid.UUID, slug string) (*model.Project, error)
}

// ProjectDeleter provides project deletion.
type ProjectDeleter interface {
	Delete(ctx context.Context, id uuid.UUID) error
}

// MemoryIDLister lists all non-deleted memory IDs in a namespace.
type MemoryIDLister interface {
	ListIDsByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]uuid.UUID, error)
}

// MemoryBulkDeleter hard-deletes all memories in a namespace.
type MemoryBulkDeleter interface {
	HardDeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error
}

// EntityBulkDeleter deletes all entities (and aliases) in a namespace.
type EntityBulkDeleter interface {
	DeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error
}

// RelationshipBulkDeleter deletes all relationships in a namespace.
type RelationshipBulkDeleter interface {
	DeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error
}

// TokenUsageReassigner reassigns token usage records from one project to another.
type TokenUsageReassigner interface {
	ReassignProject(ctx context.Context, fromProjectID, toProjectID uuid.UUID) error
}

// IngestionLogDeleter deletes all ingestion log entries for a namespace.
type IngestionLogDeleter interface {
	DeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error
}

// MemoryShareDeleter deletes all memory shares involving a namespace.
type MemoryShareDeleter interface {
	DeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error
}

// EnrichmentBulkDeleter deletes enrichment queue entries by namespace and memory.
type EnrichmentBulkDeleter interface {
	DeleteByMemoryID(ctx context.Context, memoryID uuid.UUID) error
	DeleteByNamespace(ctx context.Context, namespaceID uuid.UUID) error
}

// NamespaceDeleter deletes a namespace.
type NamespaceDeleter interface {
	Delete(ctx context.Context, id uuid.UUID) error
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

// ProjectDeleteService orchestrates recursive deletion of a project and all
// associated data. Project deletion is strictly self-service: only the project
// owner can delete their own projects.
type ProjectDeleteService struct {
	projectGetter       ProjectDeleteGetter
	projectDeleter      ProjectDeleter
	memoryIDLister      MemoryIDLister
	memoryBulkDeleter   MemoryBulkDeleter
	vectorStore         VectorDeleter
	entityDeleter       EntityBulkDeleter
	relationshipDeleter RelationshipBulkDeleter
	relationshipCleaner RelationshipCleaner
	lineageCleaner      LineageCleaner
	enrichmentDeleter   EnrichmentBulkDeleter
	tokenUsageReassign  TokenUsageReassigner
	tokenUsageCleaner   TokenUsageCleaner
	ingestionDeleter    IngestionLogDeleter
	shareDeleter        MemoryShareDeleter
	namespaceDeleter    NamespaceDeleter
	eventBus            events.EventBus
}

// NewProjectDeleteService creates a new ProjectDeleteService with the given dependencies.
func NewProjectDeleteService(
	projectGetter ProjectDeleteGetter,
	projectDeleter ProjectDeleter,
	memoryIDLister MemoryIDLister,
	memoryBulkDeleter MemoryBulkDeleter,
	vectorStore VectorDeleter,
	entityDeleter EntityBulkDeleter,
	relationshipDeleter RelationshipBulkDeleter,
	relationshipCleaner RelationshipCleaner,
	lineageCleaner LineageCleaner,
	enrichmentDeleter EnrichmentBulkDeleter,
	tokenUsageReassign TokenUsageReassigner,
	tokenUsageCleaner TokenUsageCleaner,
	ingestionDeleter IngestionLogDeleter,
	shareDeleter MemoryShareDeleter,
	namespaceDeleter NamespaceDeleter,
	eventBus events.EventBus,
) *ProjectDeleteService {
	return &ProjectDeleteService{
		projectGetter:       projectGetter,
		projectDeleter:      projectDeleter,
		memoryIDLister:      memoryIDLister,
		memoryBulkDeleter:   memoryBulkDeleter,
		vectorStore:         vectorStore,
		entityDeleter:       entityDeleter,
		relationshipDeleter: relationshipDeleter,
		relationshipCleaner: relationshipCleaner,
		lineageCleaner:      lineageCleaner,
		enrichmentDeleter:   enrichmentDeleter,
		tokenUsageReassign:  tokenUsageReassign,
		tokenUsageCleaner:   tokenUsageCleaner,
		ingestionDeleter:    ingestionDeleter,
		shareDeleter:        shareDeleter,
		namespaceDeleter:    namespaceDeleter,
		eventBus:            eventBus,
	}
}

// Delete recursively deletes a project and all associated data.
// The project's slug must not be "global". Token usage records are reassigned
// to the global project under the same owner rather than deleted.
func (s *ProjectDeleteService) Delete(ctx context.Context, req *ProjectDeleteRequest) (*ProjectDeleteResponse, error) {
	start := time.Now()

	// 1. Validate project exists and slug != "global".
	project, err := s.projectGetter.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}
	if project.Slug == "global" {
		return nil, fmt.Errorf("the global project cannot be deleted")
	}

	// 2. Look up global project for token usage reassignment.
	globalProject, err := s.projectGetter.GetBySlug(ctx, project.OwnerNamespaceID, "global")
	if err != nil {
		log.Printf("project delete: failed to find global project for token reassignment: %v", err)
		// Continue without reassignment — we'll skip it below.
		globalProject = nil
	}

	// 3. List all memory IDs in the project's namespace.
	var memoryIDs []uuid.UUID
	if s.memoryIDLister != nil {
		memoryIDs, err = s.memoryIDLister.ListIDsByNamespace(ctx, project.NamespaceID)
		if err != nil {
			log.Printf("project delete: failed to list memory IDs: %v", err)
		}
	}

	// 4. For each memory: cleanup related data + vector deletion.
	for _, memID := range memoryIDs {
		if s.relationshipCleaner != nil {
			if err := s.relationshipCleaner.DeleteBySourceMemory(ctx, memID); err != nil {
				log.Printf("project delete: relationships for memory %s: %v", memID, err)
			}
		}
		if s.lineageCleaner != nil {
			if err := s.lineageCleaner.DeleteByMemoryID(ctx, memID); err != nil {
				log.Printf("project delete: lineage for memory %s: %v", memID, err)
			}
		}
		if s.enrichmentDeleter != nil {
			if err := s.enrichmentDeleter.DeleteByMemoryID(ctx, memID); err != nil {
				log.Printf("project delete: enrichment queue for memory %s: %v", memID, err)
			}
		}
		if s.tokenUsageCleaner != nil {
			if err := s.tokenUsageCleaner.DeleteByMemoryID(ctx, memID); err != nil {
				log.Printf("project delete: token usage for memory %s: %v", memID, err)
			}
		}
		if s.vectorStore != nil {
			if err := s.vectorStore.Delete(ctx, memID); err != nil {
				log.Printf("project delete: vector for memory %s: %v", memID, err)
			}
		}
	}

	// 5. Hard delete all memories in namespace (bulk).
	if s.memoryBulkDeleter != nil {
		if err := s.memoryBulkDeleter.HardDeleteByNamespace(ctx, project.NamespaceID); err != nil {
			log.Printf("project delete: hard delete memories: %v", err)
		}
	}

	// 6. Delete entities by namespace (handles aliases internally).
	if s.entityDeleter != nil {
		if err := s.entityDeleter.DeleteByNamespace(ctx, project.NamespaceID); err != nil {
			log.Printf("project delete: entities: %v", err)
		}
	}

	// 7. Delete relationships by namespace.
	if s.relationshipDeleter != nil {
		if err := s.relationshipDeleter.DeleteByNamespace(ctx, project.NamespaceID); err != nil {
			log.Printf("project delete: relationships: %v", err)
		}
	}

	// 8. Delete ingestion log by namespace.
	if s.ingestionDeleter != nil {
		if err := s.ingestionDeleter.DeleteByNamespace(ctx, project.NamespaceID); err != nil {
			log.Printf("project delete: ingestion log: %v", err)
		}
	}

	// 9. Delete memory shares by namespace.
	if s.shareDeleter != nil {
		if err := s.shareDeleter.DeleteByNamespace(ctx, project.NamespaceID); err != nil {
			log.Printf("project delete: memory shares: %v", err)
		}
	}

	// 10. Delete enrichment queue by namespace (any remaining).
	if s.enrichmentDeleter != nil {
		if err := s.enrichmentDeleter.DeleteByNamespace(ctx, project.NamespaceID); err != nil {
			log.Printf("project delete: enrichment queue: %v", err)
		}
	}

	// 11. Reassign token_usage from deleted project to global project.
	if s.tokenUsageReassign != nil && globalProject != nil {
		if err := s.tokenUsageReassign.ReassignProject(ctx, project.ID, globalProject.ID); err != nil {
			log.Printf("project delete: token usage reassign: %v", err)
		}
	}

	// 12. Delete project record.
	if err := s.projectDeleter.Delete(ctx, project.ID); err != nil {
		return nil, fmt.Errorf("project delete: %w", err)
	}

	// 13. Delete namespace record.
	if s.namespaceDeleter != nil {
		if err := s.namespaceDeleter.Delete(ctx, project.NamespaceID); err != nil {
			log.Printf("project delete: namespace: %v", err)
		}
	}

	// 14. Emit project.deleted event.
	if s.eventBus != nil {
		data, _ := json.Marshal(map[string]interface{}{
			"project_id":   project.ID.String(),
			"project_slug": project.Slug,
			"memories":     len(memoryIDs),
		})
		events.Emit(ctx, s.eventBus, events.ProjectDeleted, "project:"+project.ID.String(), json.RawMessage(data))
	}

	log.Printf("project delete: %s (%s) completed in %v — %d memories removed",
		project.Slug, project.ID, time.Since(start), len(memoryIDs))

	return &ProjectDeleteResponse{
		DeletedMemories: len(memoryIDs),
		ProjectSlug:     project.Slug,
	}, nil
}
