package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// BatchStoreItem represents a single item in a batch store request.
//
// Importance is optional per-item; nil falls through to the documented
// default of 0.5. Confidence is not exposed for the same reason as the
// single-store path: it is an internal signal driven by reinforcement,
// decay, and contradiction haircuts.
type BatchStoreItem struct {
	Content    string          `json:"content"`
	Source     string          `json:"source"`
	Tags       []string        `json:"tags"`
	Importance *float64        `json:"importance,omitempty"`
	Metadata   json.RawMessage `json:"metadata"`
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

// BatchStoreService persists multiple memories and enqueues one enrichment
// job per successful insert. Embedding, vector upsert, and token-usage
// recording are handled async by the enrichment worker.
type BatchStoreService struct {
	memories        MemoryRepository
	projects        ProjectRepository
	namespaces      NamespaceRepository
	ingestionLogs   IngestionLogRepository
	enrichmentQueue EnrichmentQueueRepository
	settings        *SettingsService
}

// NewBatchStoreService creates a new BatchStoreService with the given dependencies.
// settings may be nil; the per-request item cap then falls through to the
// registered default for SettingAPIBatchStoreMaxItems.
func NewBatchStoreService(
	memories MemoryRepository,
	projects ProjectRepository,
	namespaces NamespaceRepository,
	ingestionLogs IngestionLogRepository,
	enrichmentQueue EnrichmentQueueRepository,
	settings *SettingsService,
) *BatchStoreService {
	return &BatchStoreService{
		memories:        memories,
		projects:        projects,
		namespaces:      namespaces,
		ingestionLogs:   ingestionLogs,
		enrichmentQueue: enrichmentQueue,
		settings:        settings,
	}
}

// BatchStore persists items independently; failure of one item does not
// affect others.
func (s *BatchStoreService) BatchStore(ctx context.Context, req *BatchStoreRequest) (*BatchStoreResponse, error) {
	start := time.Now()

	// Validate required fields.
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}
	if len(req.Items) == 0 {
		return nil, fmt.Errorf("items must not be empty")
	}
	maxItems := s.settings.ResolveIntWithDefault(ctx, SettingAPIBatchStoreMaxItems, "global")
	if len(req.Items) > maxItems {
		return nil, fmt.Errorf("too many items: %d exceeds maximum of %d", len(req.Items), maxItems)
	}
	if req.Options.Extract {
		return nil, fmt.Errorf("extract support is not yet implemented")
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

	_ = project // retained for future attribution fields on the response

	// Process each item independently.
	errs := []BatchStoreError{}
	memoriesCreated := 0
	// In-batch dedup: collapse items whose content hash already appeared earlier
	// in the same batch so we do not race ourselves into duplicates.
	seenHashes := make(map[string]uuid.UUID, len(req.Items))

	for i, item := range req.Items {
		hash := storage.HashContent(item.Content)

		// Same-batch collision: an earlier item already created (or matched) this
		// content. Skip the insert.
		if _, ok := seenHashes[hash]; ok {
			slog.Info("batch_store: dedup hit (in-batch)",
				"namespace", ns.ID, "index", i, "hash", hash)
			continue
		}

		// Cross-batch collision: a live row already has this content. Skip.
		existing, lookupErr := s.memories.LookupByContentHash(ctx, ns.ID, hash)
		if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
			errs = append(errs, BatchStoreError{
				Index:   i,
				Message: fmt.Sprintf("dedup lookup: %v", lookupErr),
			})
			continue
		}
		if existing != nil {
			slog.Info("batch_store: dedup hit",
				"namespace", ns.ID, "index", i, "memory", existing.ID, "hash", hash)
			seenHashes[hash] = existing.ID
			continue
		}

		memID := uuid.New()
		now := time.Now()

		var source *string
		if item.Source != "" {
			source = &item.Source
		}
		importance := 0.5
		if item.Importance != nil {
			importance = *item.Importance
		}
		mem := &model.Memory{
			ID:          memID,
			NamespaceID: ns.ID,
			Content:     item.Content,
			ContentHash: hash,
			Source:      source,
			Tags:        item.Tags,
			Confidence:  1.0,
			Importance:  importance,
			Metadata:    item.Metadata,
			CreatedAt:   now,
			UpdatedAt:   now,
			ExpiresAt:   expiresAt,
		}

		// Persist the memory.
		if err := s.memories.Create(ctx, mem); err != nil {
			errs = append(errs, BatchStoreError{
				Index:   i,
				Message: fmt.Sprintf("failed to create memory: %v", err),
			})
			continue
		}

		seenHashes[hash] = memID
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

	latency := time.Since(start).Milliseconds()

	return &BatchStoreResponse{
		Processed:       len(req.Items),
		MemoriesCreated: memoriesCreated,
		Errors:          errs,
		LatencyMs:       latency,
	}, nil
}
