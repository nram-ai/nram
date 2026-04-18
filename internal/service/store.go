package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// bestEmbeddingDimension forwards to storage.BestEmbeddingDimension so existing
// service-package call sites keep the short name. Single source of truth lives
// beside SupportedVectorDimensions in the storage package.
func bestEmbeddingDimension(providerDims []int) int {
	return storage.BestEmbeddingDimension(providerDims)
}

// MemoryRepository defines the memory persistence operations needed by the store service.
type MemoryRepository interface {
	Create(ctx context.Context, mem *model.Memory) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
}

// ProjectRepository defines the project lookup operations needed by the store service.
type ProjectRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// NamespaceRepository defines the namespace lookup operations needed by the store service.
type NamespaceRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error)
}

// IngestionLogRepository defines the ingestion log persistence operations needed by the store service.
type IngestionLogRepository interface {
	Create(ctx context.Context, log *model.IngestionLog) error
}

// TokenUsageRepository defines the token usage recording operations needed by the store service.
type TokenUsageRepository interface {
	Record(ctx context.Context, usage *model.TokenUsage) error
}

// EnrichmentQueueRepository defines the enrichment queue operations needed by the store service.
type EnrichmentQueueRepository interface {
	Enqueue(ctx context.Context, item *model.EnrichmentJob) error
}

// VectorStoreWriter defines the vector persistence operations needed by the store service.
type VectorStoreWriter interface {
	Upsert(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error
}

// StoreOptions controls optional behavior during memory storage.
type StoreOptions struct {
	Enrich  bool   `json:"enrich"`
	Extract bool   `json:"extract"`
	TTL     string `json:"ttl"` // e.g., "30d", "7d", "24h"
}

// StoreRequest contains all parameters needed to store a new memory.
type StoreRequest struct {
	ProjectID uuid.UUID       `json:"project_id"`
	Content   string          `json:"content"`
	Source    string          `json:"source"`
	Tags     []string        `json:"tags"`
	Metadata json.RawMessage `json:"metadata"`
	Options  StoreOptions    `json:"options"`
	// Caller context (set by handler/middleware)
	UserID   *uuid.UUID `json:"-"`
	OrgID    *uuid.UUID `json:"-"`
	APIKeyID *uuid.UUID `json:"-"`
}

// StoreResponse contains the result of a memory store operation.
type StoreResponse struct {
	ID               uuid.UUID `json:"id"`
	ProjectID        uuid.UUID `json:"project_id"`
	ProjectSlug      string    `json:"project_slug"`
	Path             string    `json:"path"`
	Content          string    `json:"content"`
	Tags             []string  `json:"tags"`
	Enriched         bool      `json:"enriched"`
	EnrichmentQueued bool      `json:"enrichment_queued,omitempty"`
	LatencyMs        int64     `json:"latency_ms"`
}

// StoreService persists a memory, logs ingestion, and enqueues an enrichment
// job. Embedding, vector upsert, and token-usage recording happen async on
// the enrichment worker so the write path never blocks on an embed provider.
type StoreService struct {
	memories        MemoryRepository
	projects        ProjectRepository
	namespaces      NamespaceRepository
	ingestionLogs   IngestionLogRepository
	enrichmentQueue EnrichmentQueueRepository
}

// NewStoreService creates a new StoreService with the given dependencies.
func NewStoreService(
	memories MemoryRepository,
	projects ProjectRepository,
	namespaces NamespaceRepository,
	ingestionLogs IngestionLogRepository,
	enrichmentQueue EnrichmentQueueRepository,
) *StoreService {
	return &StoreService{
		memories:        memories,
		projects:        projects,
		namespaces:      namespaces,
		ingestionLogs:   ingestionLogs,
		enrichmentQueue: enrichmentQueue,
	}
}

// Store persists the memory and enqueues one enrichment job.
func (s *StoreService) Store(ctx context.Context, req *StoreRequest) (*StoreResponse, error) {
	start := time.Now()

	// Validate required fields.
	if strings.TrimSpace(req.Content) == "" {
		return nil, fmt.Errorf("content is required")
	}
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}

	if req.Options.Extract {
		return nil, fmt.Errorf("extract support is not yet implemented")
	}

	// Look up project.
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	// Look up project's namespace.
	ns, err := s.namespaces.GetByID(ctx, project.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("namespace not found: %w", err)
	}

	// Parse TTL if provided.
	var expiresAt *time.Time
	if req.Options.TTL != "" {
		ttlDur, err := parseTTL(req.Options.TTL)
		if err != nil {
			return nil, fmt.Errorf("invalid TTL %q: %w", req.Options.TTL, err)
		}
		t := time.Now().Add(ttlDur)
		expiresAt = &t
	}

	// Build memory model.
	memID := uuid.New()
	now := time.Now()
	var source *string
	if req.Source != "" {
		source = &req.Source
	}

	mem := &model.Memory{
		ID:          memID,
		NamespaceID: ns.ID,
		Content:     req.Content,
		Source:      source,
		Tags:        req.Tags,
		Confidence:  1.0,
		Importance:  0.5,
		Metadata:    req.Metadata,
		CreatedAt:   now,
		UpdatedAt:   now,
		ExpiresAt:   expiresAt,
	}

	// Persist the memory.
	if err := s.memories.Create(ctx, mem); err != nil {
		return nil, fmt.Errorf("failed to create memory: %w", err)
	}

	// Create ingestion log.
	ingLog := &model.IngestionLog{
		ID:          uuid.New(),
		NamespaceID: ns.ID,
		Source:      req.Source,
		RawContent:  req.Content,
		MemoryIDs:   []uuid.UUID{memID},
		Status:      "completed",
		Metadata:    req.Metadata,
		CreatedAt:   time.Now(),
	}
	_ = s.ingestionLogs.Create(ctx, ingLog)

	enrichmentQueued := false
	job := &model.EnrichmentJob{
		ID:          uuid.New(),
		MemoryID:    memID,
		NamespaceID: ns.ID,
		Status:      "pending",
		Priority:    0,
		Attempts:    0,
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := s.enrichmentQueue.Enqueue(ctx, job); err == nil {
		enrichmentQueued = true
	}

	latency := time.Since(start).Milliseconds()

	tags := req.Tags
	if tags == nil {
		tags = []string{}
	}

	return &StoreResponse{
		ID:               memID,
		ProjectID:        project.ID,
		ProjectSlug:      project.Slug,
		Path:             ns.Path,
		Content:          req.Content,
		Tags:             tags,
		Enriched:         false,
		EnrichmentQueued: enrichmentQueued,
		LatencyMs:        latency,
	}, nil
}

// parseTTL parses a TTL string like "30d", "7d", "24h", "1h" into a time.Duration.
// Supported units: "d" (days), "h" (hours), "m" (minutes), "s" (seconds).
func parseTTL(ttl string) (time.Duration, error) {
	ttl = strings.TrimSpace(ttl)
	if ttl == "" {
		return 0, fmt.Errorf("empty TTL")
	}

	// Find where the numeric part ends and the unit begins.
	i := 0
	for i < len(ttl) && (ttl[i] >= '0' && ttl[i] <= '9') {
		i++
	}

	if i == 0 {
		return 0, fmt.Errorf("no numeric value in TTL %q", ttl)
	}
	if i == len(ttl) {
		return 0, fmt.Errorf("no unit in TTL %q", ttl)
	}

	numStr := ttl[:i]
	unit := ttl[i:]

	n, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value in TTL %q: %w", ttl, err)
	}

	if n <= 0 {
		return 0, fmt.Errorf("TTL value must be positive, got %d", n)
	}

	switch unit {
	case "d":
		return time.Duration(n) * 24 * time.Hour, nil
	case "h":
		return time.Duration(n) * time.Hour, nil
	case "m":
		return time.Duration(n) * time.Minute, nil
	case "s":
		return time.Duration(n) * time.Second, nil
	default:
		return 0, fmt.Errorf("unsupported TTL unit %q; use d, h, m, or s", unit)
	}
}
