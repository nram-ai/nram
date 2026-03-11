package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// GraphEntity represents an entity node for graph visualization.
type GraphEntity struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Canonical    string   `json:"canonical"`
	EntityType   string   `json:"entity_type"`
	MentionCount int      `json:"mention_count"`
	Aliases      []string `json:"aliases"`
	CreatedAt    string   `json:"created_at"`
	UpdatedAt    string   `json:"updated_at"`
}

// GraphRelationship represents a relationship edge for graph visualization.
type GraphRelationship struct {
	ID       string  `json:"id"`
	SourceID string  `json:"source_id"`
	TargetID string  `json:"target_id"`
	Relation string  `json:"relation"`
	Weight   float64 `json:"weight"`
}

// GraphResponse is the response payload for the admin graph endpoint.
type GraphResponse struct {
	Entities      []GraphEntity       `json:"entities"`
	Relationships []GraphRelationship `json:"relationships"`
}

// GraphProjectStore retrieves a project by ID.
type GraphProjectStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// GraphEntityStore retrieves entities for a namespace.
type GraphEntityStore interface {
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Entity, error)
}

// GraphRelationshipStore retrieves relationships for a namespace.
type GraphRelationshipStore interface {
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Relationship, error)
}

// GraphAliasStore retrieves aliases for an entity.
type GraphAliasStore interface {
	ListByEntity(ctx context.Context, entityID uuid.UUID) ([]model.EntityAlias, error)
}

// GraphAdminConfig holds dependencies for the admin graph handler.
type GraphAdminConfig struct {
	Projects      GraphProjectStore
	Entities      GraphEntityStore
	Relationships GraphRelationshipStore
	Aliases       GraphAliasStore
}

// NewAdminGraphHandler returns an http.HandlerFunc that serves graph data
// for a given project. Query parameter: project (project UUID).
func NewAdminGraphHandler(cfg GraphAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "method not allowed",
			})
			return
		}

		projectIDStr := r.URL.Query().Get("project")
		if projectIDStr == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "project query parameter is required",
			})
			return
		}

		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid project ID",
			})
			return
		}

		project, err := cfg.Projects.GetByID(r.Context(), projectID)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "project not found",
			})
			return
		}

		entities, err := cfg.Entities.ListByNamespace(r.Context(), project.NamespaceID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve entities",
			})
			return
		}

		relationships, err := cfg.Relationships.ListByNamespace(r.Context(), project.NamespaceID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve relationships",
			})
			return
		}

		// Build entity ID set for filtering relationships to only those
		// connecting entities in this namespace.
		entityIDs := make(map[string]bool, len(entities))
		graphEntities := make([]GraphEntity, 0, len(entities))

		for _, e := range entities {
			eid := e.ID.String()
			entityIDs[eid] = true

			aliases, _ := cfg.Aliases.ListByEntity(r.Context(), e.ID)
			aliasNames := make([]string, 0, len(aliases))
			for _, a := range aliases {
				aliasNames = append(aliasNames, a.Alias)
			}

			graphEntities = append(graphEntities, GraphEntity{
				ID:           eid,
				Name:         e.Name,
				Canonical:    e.Canonical,
				EntityType:   e.EntityType,
				MentionCount: e.MentionCount,
				Aliases:      aliasNames,
				CreatedAt:    e.CreatedAt.Format("2006-01-02T15:04:05Z"),
				UpdatedAt:    e.UpdatedAt.Format("2006-01-02T15:04:05Z"),
			})
		}

		graphRelationships := make([]GraphRelationship, 0, len(relationships))
		for _, rel := range relationships {
			srcID := rel.SourceID.String()
			tgtID := rel.TargetID.String()
			// Only include relationships where both ends are in the entity set.
			if entityIDs[srcID] && entityIDs[tgtID] {
				graphRelationships = append(graphRelationships, GraphRelationship{
					ID:       rel.ID.String(),
					SourceID: srcID,
					TargetID: tgtID,
					Relation: rel.Relation,
					Weight:   rel.Weight,
				})
			}
		}

		writeJSON(w, http.StatusOK, GraphResponse{
			Entities:      graphEntities,
			Relationships: graphRelationships,
		})
	}
}
