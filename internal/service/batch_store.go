package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// BatchStoreItem represents a single item in a batch store request.
type BatchStoreItem struct {
	Content  string          `json:"content"`
	Source   string          `json:"source"`
	Tags     []string        `json:"tags"`
	Metadata json.RawMessage `json:"metadata"`
}

// BatchStoreRequest contains all parameters needed for a batch memory store operation.
type BatchStoreRequest struct {
	ProjectID uuid.UUID        `json:"project_id"`
	Items     []BatchStoreItem `json:"items"`
	Options   StoreOptions     `json:"options"`
	// Caller context (set by handler/middleware)
	UserID   *uuid.UUID `json:"-"`
	OrgID    *uuid.UUID `json:"-"`
	APIKeyID *uuid.UUID `json:"-"`
}

// BatchStoreError represents a per-item error in a batch store operation.
type BatchStoreError struct {
	Index   int    `json:"index"`
	Message string `json:"message"`
}

// BatchStoreResponse contains the result of a batch store operation.
type BatchStoreResponse struct {
	Processed       int               `json:"processed"`
	MemoriesCreated int               `json:"memories_created"`
	Errors          []BatchStoreError `json:"errors"`
	LatencyMs       int64             `json:"latency_ms"`
}

// BatchStoreService orchestrates batch memory creation with parallel embedding,
// per-item independent transactions, and per-item error reporting.
type BatchStoreService struct {
	memories        MemoryRepository
	projects        ProjectRepository
	namespaces      NamespaceRepository
	ingestionLogs   IngestionLogRepository
	tokenUsage      TokenUsageRepository
	enrichmentQueue EnrichmentQueueRepository
	vectorStore     VectorStoreWriter
	embedProvider   func() provider.EmbeddingProvider
}

// NewBatchStoreService creates a new BatchStoreService with the given dependencies.
func NewBatchStoreService(
	memories MemoryRepository,
	projects ProjectRepository,
	namespaces NamespaceRepository,
	ingestionLogs IngestionLogRepository,
	tokenUsage TokenUsageRepository,
	enrichmentQueue EnrichmentQueueRepository,
	vectorStore VectorStoreWriter,
	embedProvider func() provider.EmbeddingProvider,
) *BatchStoreService {
	return &BatchStoreService{
		memories:        memories,
		projects:        projects,
		namespaces:      namespaces,
		ingestionLogs:   ingestionLogs,
		tokenUsage:      tokenUsage,
		enrichmentQueue: enrichmentQueue,
		vectorStore:     vectorStore,
		embedProvider:   embedProvider,
	}
}

// maxBatchItems is the maximum number of items allowed in a single batch store request.
const maxBatchItems = 100

// BatchStore creates multiple memories in a single batch operation with optional embedding.
// Each item is processed independently; failure of one item does not affect others.
func (s *BatchStoreService) BatchStore(ctx context.Context, req *BatchStoreRequest) (*BatchStoreResponse, error) {
	start := time.Now()

	// Validate required fields.
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}
	if len(req.Items) == 0 {
		return nil, fmt.Errorf("items must not be empty")
	}
	if len(req.Items) > maxBatchItems {
		return nil, fmt.Errorf("too many items: %d exceeds maximum of %d", len(req.Items), maxBatchItems)
	}

	// Look up project (once for all items).
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	// Look up project's namespace (once for all items).
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

	// Batch embed ALL contents at once if a provider is available.
	var embeddings [][]float32
	var embeddingDone bool
	var embeddingUsage *provider.TokenUsage
	var embeddingModel string
	var embeddingProviderName string

	if s.embedProvider != nil {
		ep := s.embedProvider()
		if ep != nil {
			dims := ep.Dimensions()
			dim := 0
			if len(dims) > 0 {
				dim = dims[0]
			}

			inputs := make([]string, len(req.Items))
			for i, item := range req.Items {
				inputs[i] = item.Content
			}

			embReq := &provider.EmbeddingRequest{
				Input:     inputs,
				Dimension: dim,
			}

			resp, embErr := ep.Embed(ctx, embReq)
			if embErr == nil && len(resp.Embeddings) == len(req.Items) {
				embeddingDone = true
				embeddings = resp.Embeddings
				embeddingUsage = &resp.Usage
				embeddingModel = resp.Model
				embeddingProviderName = ep.Name()
			}
			// On embedding error or length mismatch, continue without embeddings.
		}
	}

	// Process each item independently.
	errors := []BatchStoreError{}
	memoriesCreated := 0

	for i, item := range req.Items {
		memID := uuid.New()
		now := time.Now()

		var source *string
		if item.Source != "" {
			source = &item.Source
		}

		mem := &model.Memory{
			ID:          memID,
			NamespaceID: ns.ID,
			Content:     item.Content,
			Source:      source,
			Tags:        item.Tags,
			Confidence:  1.0,
			Importance:  0.5,
			Metadata:    item.Metadata,
			CreatedAt:   now,
			UpdatedAt:   now,
			ExpiresAt:   expiresAt,
		}

		// Set embedding dimension if we have embeddings for this item.
		if embeddingDone && i < len(embeddings) {
			embDim := len(embeddings[i])
			mem.EmbeddingDim = &embDim

			if s.vectorStore != nil {
				if err := s.vectorStore.Upsert(ctx, memID, ns.ID, embeddings[i], embDim); err != nil {
					errors = append(errors, BatchStoreError{
						Index:   i,
						Message: fmt.Sprintf("vector upsert failed: %v", err),
					})
					continue
				}
			}
		}

		// Persist the memory.
		if err := s.memories.Create(ctx, mem); err != nil {
			errors = append(errors, BatchStoreError{
				Index:   i,
				Message: fmt.Sprintf("failed to create memory: %v", err),
			})
			continue
		}

		memoriesCreated++

		// Create ingestion log for this item.
		ingLog := &model.IngestionLog{
			ID:          uuid.New(),
			NamespaceID: ns.ID,
			Source:      item.Source,
			RawContent:  item.Content,
			MemoryIDs:   []uuid.UUID{memID},
			Status:      "completed",
			Metadata:    item.Metadata,
			CreatedAt:   time.Now(),
		}
		_ = s.ingestionLogs.Create(ctx, ingLog)

		// Queue enrichment if requested.
		if req.Options.Enrich {
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
			_ = s.enrichmentQueue.Enqueue(ctx, job)
		}
	}

	// Record token usage once for the entire batch embedding call.
	if embeddingDone && embeddingUsage != nil {
		projectID := project.ID
		usage := &model.TokenUsage{
			ID:           uuid.New(),
			OrgID:        req.OrgID,
			UserID:       req.UserID,
			ProjectID:    &projectID,
			NamespaceID:  ns.ID,
			Operation:    "embedding",
			Provider:     embeddingProviderName,
			Model:        embeddingModel,
			TokensInput:  embeddingUsage.PromptTokens,
			TokensOutput: embeddingUsage.CompletionTokens,
			APIKeyID:     req.APIKeyID,
			CreatedAt:    time.Now(),
		}
		_ = s.tokenUsage.Record(ctx, usage)
	}

	latency := time.Since(start).Milliseconds()

	return &BatchStoreResponse{
		Processed:       len(req.Items),
		MemoriesCreated: memoriesCreated,
		Errors:          errors,
		LatencyMs:       latency,
	}, nil
}
