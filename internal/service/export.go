package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// ExportFormat defines the output format for project data export.
type ExportFormat string

const (
	ExportFormatJSON   ExportFormat = "json"
	ExportFormatNDJSON ExportFormat = "ndjson"
)

// EntityLister provides listing of entities within a namespace.
type EntityLister interface {
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Entity, error)
}

// RelationshipLister provides listing of relationships for an entity.
type RelationshipLister interface {
	ListByEntity(ctx context.Context, entityID uuid.UUID) ([]model.Relationship, error)
}

// LineageReader provides listing of lineage records for a memory.
type LineageReader interface {
	ListByMemory(ctx context.Context, namespaceID uuid.UUID, memoryID uuid.UUID) ([]model.MemoryLineage, error)
}

// ExportRequest contains the parameters for a project data export.
type ExportRequest struct {
	ProjectID uuid.UUID
	Format    ExportFormat
	// IncludeSuperseded surfaces rows that were superseded by a paraphrase
	// dedup or contradiction pass. Default false hides them so exports don't
	// ship duplicate-loser rows downstream.
	IncludeSuperseded bool
}

// ExportData holds the complete export payload for JSON format.
type ExportData struct {
	Version       string               `json:"version"`
	ExportedAt    time.Time            `json:"exported_at"`
	Project       ExportProject        `json:"project"`
	Memories      []ExportMemory       `json:"memories"`
	Entities      []ExportEntity       `json:"entities"`
	Relationships []ExportRelationship `json:"relationships"`
	Stats         ExportStats          `json:"stats"`
}

// ExportProject is a minimal project representation for export.
type ExportProject struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
	Slug string    `json:"slug"`
}

// ExportMemory is a memory representation for export.
type ExportMemory struct {
	ID         uuid.UUID       `json:"id"`
	Content    string          `json:"content"`
	Tags       []string        `json:"tags"`
	Source     *string         `json:"source,omitempty"`
	Confidence float64         `json:"confidence"`
	Importance float64         `json:"importance"`
	Enriched   bool            `json:"enriched"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	Lineage    []ExportLineage `json:"lineage,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

// ExportEntity is an entity representation for export.
type ExportEntity struct {
	ID           uuid.UUID       `json:"id"`
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	Canonical    string          `json:"canonical"`
	Properties   json.RawMessage `json:"properties,omitempty"`
	MentionCount int             `json:"mention_count"`
}

// ExportRelationship is a relationship representation for export.
type ExportRelationship struct {
	ID         uuid.UUID  `json:"id"`
	SourceID   uuid.UUID  `json:"source_id"`
	TargetID   uuid.UUID  `json:"target_id"`
	Relation   string     `json:"relation"`
	Weight     float64    `json:"weight"`
	ValidFrom  time.Time  `json:"valid_from"`
	ValidUntil *time.Time `json:"valid_until,omitempty"`
}

// ExportLineage is a lineage record representation for export.
type ExportLineage struct {
	ParentID *uuid.UUID `json:"parent_id,omitempty"`
	Relation string     `json:"relation"`
}

// ExportStats contains aggregate counts for the export.
type ExportStats struct {
	MemoryCount       int `json:"memory_count"`
	EntityCount       int `json:"entity_count"`
	RelationshipCount int `json:"relationship_count"`
}

// ExportService handles exporting project data in various formats.
type ExportService struct {
	memories      MemoryReader
	entities      EntityLister
	relationships RelationshipLister
	lineage       LineageReader
	projects      ProjectRepository
}

// NewExportService creates a new ExportService with the given dependencies.
func NewExportService(
	memories MemoryReader,
	entities EntityLister,
	relationships RelationshipLister,
	lineage LineageReader,
	projects ProjectRepository,
) *ExportService {
	return &ExportService{
		memories:      memories,
		entities:      entities,
		relationships: relationships,
		lineage:       lineage,
		projects:      projects,
	}
}

const exportVersion = "1.0"
const exportPageSize = 100

// Export collects all project data and returns it as an ExportData struct (JSON format).
func (s *ExportService) Export(ctx context.Context, req *ExportRequest) (*ExportData, error) {
	if req == nil {
		return nil, fmt.Errorf("export request is required")
	}
	if req.Format != ExportFormatJSON && req.Format != "" {
		return nil, fmt.Errorf("unsupported export format %q; use %q or %q", req.Format, ExportFormatJSON, ExportFormatNDJSON)
	}

	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	// Collect all memories via pagination.
	allMemories, err := s.collectAllMemories(ctx, project.NamespaceID, req.IncludeSuperseded)
	if err != nil {
		return nil, fmt.Errorf("failed to list memories: %w", err)
	}

	// Collect lineage for each memory.
	exportMemories := make([]ExportMemory, 0, len(allMemories))
	for _, mem := range allMemories {
		em := toExportMemory(mem)
		lineageRecords, err := s.lineage.ListByMemory(ctx, mem.NamespaceID, mem.ID)
		if err == nil && len(lineageRecords) > 0 {
			em.Lineage = make([]ExportLineage, 0, len(lineageRecords))
			for _, lr := range lineageRecords {
				em.Lineage = append(em.Lineage, ExportLineage{
					ParentID: lr.ParentID,
					Relation: lr.Relation,
				})
			}
		}
		exportMemories = append(exportMemories, em)
	}

	// Collect all entities.
	allEntities, err := s.entities.ListByNamespace(ctx, project.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list entities: %w", err)
	}

	exportEntities := make([]ExportEntity, 0, len(allEntities))
	for _, ent := range allEntities {
		exportEntities = append(exportEntities, toExportEntity(ent))
	}

	// Collect relationships for each entity (deduplicated).
	seenRels := make(map[uuid.UUID]struct{})
	var exportRels []ExportRelationship
	for _, ent := range allEntities {
		rels, err := s.relationships.ListByEntity(ctx, ent.ID)
		if err != nil {
			continue
		}
		for _, rel := range rels {
			if _, ok := seenRels[rel.ID]; ok {
				continue
			}
			seenRels[rel.ID] = struct{}{}
			exportRels = append(exportRels, toExportRelationship(rel))
		}
	}

	if exportMemories == nil {
		exportMemories = []ExportMemory{}
	}
	if exportEntities == nil {
		exportEntities = []ExportEntity{}
	}
	if exportRels == nil {
		exportRels = []ExportRelationship{}
	}

	return &ExportData{
		Version:    exportVersion,
		ExportedAt: time.Now(),
		Project: ExportProject{
			ID:   project.ID,
			Name: project.Name,
			Slug: project.Slug,
		},
		Memories:      exportMemories,
		Entities:      exportEntities,
		Relationships: exportRels,
		Stats: ExportStats{
			MemoryCount:       len(exportMemories),
			EntityCount:       len(exportEntities),
			RelationshipCount: len(exportRels),
		},
	}, nil
}

// ndjsonRecord is a wrapper that adds a "type" field to streamed NDJSON records.
type ndjsonRecord struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// ExportNDJSON streams project data as newline-delimited JSON to the given writer.
func (s *ExportService) ExportNDJSON(ctx context.Context, req *ExportRequest, w io.Writer) error {
	if req == nil {
		return fmt.Errorf("export request is required")
	}

	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return fmt.Errorf("project not found: %w", err)
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	// First line: project metadata.
	if err := enc.Encode(ndjsonRecord{
		Type: "project",
		Data: ExportProject{
			ID:   project.ID,
			Name: project.Name,
			Slug: project.Slug,
		},
	}); err != nil {
		return fmt.Errorf("failed to write project record: %w", err)
	}

	// Stream memories.
	allMemories, err := s.collectAllMemories(ctx, project.NamespaceID, req.IncludeSuperseded)
	if err != nil {
		return fmt.Errorf("failed to list memories: %w", err)
	}

	for _, mem := range allMemories {
		em := toExportMemory(mem)
		lineageRecords, lErr := s.lineage.ListByMemory(ctx, mem.NamespaceID, mem.ID)
		if lErr == nil && len(lineageRecords) > 0 {
			em.Lineage = make([]ExportLineage, 0, len(lineageRecords))
			for _, lr := range lineageRecords {
				em.Lineage = append(em.Lineage, ExportLineage{
					ParentID: lr.ParentID,
					Relation: lr.Relation,
				})
			}
		}
		if err := enc.Encode(ndjsonRecord{Type: "memory", Data: em}); err != nil {
			return fmt.Errorf("failed to write memory record: %w", err)
		}
	}

	// Stream entities.
	allEntities, err := s.entities.ListByNamespace(ctx, project.NamespaceID)
	if err != nil {
		return fmt.Errorf("failed to list entities: %w", err)
	}

	for _, ent := range allEntities {
		if err := enc.Encode(ndjsonRecord{Type: "entity", Data: toExportEntity(ent)}); err != nil {
			return fmt.Errorf("failed to write entity record: %w", err)
		}
	}

	// Stream relationships (deduplicated).
	seenRels := make(map[uuid.UUID]struct{})
	for _, ent := range allEntities {
		rels, rErr := s.relationships.ListByEntity(ctx, ent.ID)
		if rErr != nil {
			continue
		}
		for _, rel := range rels {
			if _, ok := seenRels[rel.ID]; ok {
				continue
			}
			seenRels[rel.ID] = struct{}{}
			if err := enc.Encode(ndjsonRecord{Type: "relationship", Data: toExportRelationship(rel)}); err != nil {
				return fmt.Errorf("failed to write relationship record: %w", err)
			}
		}
	}

	return nil
}

// collectAllMemories paginates through all memories in the given namespace.
// When includeSuperseded is false, the SQL filter drops rows with
// superseded_by set so exports don't ship duplicate losers.
func (s *ExportService) collectAllMemories(ctx context.Context, namespaceID uuid.UUID, includeSuperseded bool) ([]model.Memory, error) {
	filters := storage.MemoryListFilters{HideSuperseded: !includeSuperseded}
	all := []model.Memory{}
	offset := 0
	for {
		page, err := s.memories.ListByNamespaceFiltered(ctx, namespaceID, filters, exportPageSize, offset)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) < exportPageSize {
			break
		}
		offset += len(page)
	}
	return all, nil
}

func toExportMemory(mem model.Memory) ExportMemory {
	tags := mem.Tags
	if tags == nil {
		tags = []string{}
	}
	return ExportMemory{
		ID:         mem.ID,
		Content:    mem.Content,
		Tags:       tags,
		Source:     mem.Source,
		Confidence: mem.Confidence,
		Importance: mem.Importance,
		Enriched:   mem.Enriched,
		Metadata:   mem.Metadata,
		CreatedAt:  mem.CreatedAt,
	}
}

func toExportEntity(ent model.Entity) ExportEntity {
	return ExportEntity{
		ID:           ent.ID,
		Name:         ent.Name,
		Type:         ent.EntityType,
		Canonical:    ent.Canonical,
		Properties:   ent.Properties,
		MentionCount: ent.MentionCount,
	}
}

func toExportRelationship(rel model.Relationship) ExportRelationship {
	return ExportRelationship{
		ID:         rel.ID,
		SourceID:   rel.SourceID,
		TargetID:   rel.TargetID,
		Relation:   rel.Relation,
		Weight:     rel.Weight,
		ValidFrom:  rel.ValidFrom,
		ValidUntil: rel.ValidUntil,
	}
}
