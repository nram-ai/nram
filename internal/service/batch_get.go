package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// BatchGetRequest contains the parameters for a batch memory retrieval.
type BatchGetRequest struct {
	ProjectID uuid.UUID   `json:"project_id"`
	IDs       []uuid.UUID `json:"ids"`
	// IncludeSuperseded surfaces rows that were superseded by a paraphrase
	// dedup or contradiction pass. Default false hides them so callers don't
	// have to filter the loser side of a winner/loser pair themselves.
	IncludeSuperseded bool `json:"include_superseded,omitempty"`
}

// BatchGetResponse contains the results of a batch memory retrieval.
type BatchGetResponse struct {
	Found     []MemoryDetail `json:"found"`
	NotFound  []uuid.UUID    `json:"not_found"`
	LatencyMs int64          `json:"latency_ms"`
}

// MemoryDetail is a projection of a memory for batch get responses.
type MemoryDetail struct {
	ID        uuid.UUID       `json:"id"`
	Content   string          `json:"content"`
	Tags      []string        `json:"tags"`
	Source    *string         `json:"source,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	Enriched  bool            `json:"enriched"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// BatchGetService handles batch retrieval of memories by ID.
type BatchGetService struct {
	memories MemoryReader
	projects ProjectRepository
}

// NewBatchGetService creates a new BatchGetService with the given dependencies.
func NewBatchGetService(memories MemoryReader, projects ProjectRepository) *BatchGetService {
	return &BatchGetService{
		memories: memories,
		projects: projects,
	}
}

// BatchGet retrieves multiple memories by ID, filtering to the project's namespace
// and excluding soft-deleted records. Superseded rows are excluded by default;
// set req.IncludeSuperseded to surface them. It returns found memories and a
// list of IDs that were not found.
func (s *BatchGetService) BatchGet(ctx context.Context, req *BatchGetRequest) (*BatchGetResponse, error) {
	start := time.Now()

	// Validate required fields.
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}
	if len(req.IDs) == 0 {
		return nil, fmt.Errorf("ids must be non-empty")
	}

	// Look up the project to get its namespace.
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	// Fetch all requested memories in a single batch call.
	memories, err := s.memories.GetBatch(ctx, req.IDs)
	if err != nil {
		return nil, fmt.Errorf("batch get failed: %w", err)
	}

	// Index found memories by ID for quick lookup.
	foundMap := make(map[uuid.UUID]struct{}, len(memories))

	var found []MemoryDetail
	for _, mem := range memories {
		// Exclude memories not in the project's namespace.
		if mem.NamespaceID != project.NamespaceID {
			continue
		}
		// Exclude soft-deleted memories.
		if mem.DeletedAt != nil {
			continue
		}
		if mem.SupersededBy != nil && !req.IncludeSuperseded {
			continue
		}

		foundMap[mem.ID] = struct{}{}
		tags := mem.Tags
		if tags == nil {
			tags = []string{}
		}
		found = append(found, MemoryDetail{
			ID:        mem.ID,
			Content:   mem.Content,
			Tags:      tags,
			Source:    mem.Source,
			Metadata:  mem.Metadata,
			Enriched:  mem.Enriched,
			CreatedAt: mem.CreatedAt,
			UpdatedAt: mem.UpdatedAt,
		})
	}

	// Build not_found list from requested IDs that weren't in the found set.
	var notFound []uuid.UUID
	for _, id := range req.IDs {
		if _, ok := foundMap[id]; !ok {
			notFound = append(notFound, id)
		}
	}

	if found == nil {
		found = []MemoryDetail{}
	}
	if notFound == nil {
		notFound = []uuid.UUID{}
	}

	latency := time.Since(start).Milliseconds()

	return &BatchGetResponse{
		Found:     found,
		NotFound:  notFound,
		LatencyMs: latency,
	}, nil
}
