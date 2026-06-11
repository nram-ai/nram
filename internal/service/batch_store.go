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
	"github.com/nram-ai/nram/internal/observability/metrics"
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
	metrics         *metrics.Metrics
}

// NewBatchStoreService creates a new BatchStoreService with the given dependencies.
// settings may be nil; the per-request item cap then falls through to the
// registered default for SettingAPIBatchStoreMaxItems. Prometheus metrics are
// opt-in via WithMetrics.
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

// WithMetrics attaches the Prometheus metrics sink. Returns the same service
// for chaining at construction time.
func (s *BatchStoreService) WithMetrics(m *metrics.Metrics) *BatchStoreService {
	s.metrics = m
	return s
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

	// Collected for batched persistence after the validation/dedup loop. The
	// three slices stay index-aligned with pendingIdx (the original item index)
	// so a per-row fallback can attribute an insert error to the right item.
	pendingMems := make([]*model.Memory, 0, len(req.Items))
	pendingLogs := make([]*model.IngestionLog, 0, len(req.Items))
	pendingJobs := make([]*model.EnrichmentJob, 0, len(req.Items))
	pendingIdx := make([]int, 0, len(req.Items))

	defaultImportance := resolveDefaultImportance(ctx, s.settings)
	defaultConfidence := resolveDefaultConfidence(ctx, s.settings)

	for i, item := range req.Items {
		// "dream" is reserved for the consolidation cycle (provenance is recorded
		// via Origin=OriginDream). Reject it per-item rather than failing the batch.
		if isReservedSource(item.Source) {
			errs = append(errs, BatchStoreError{
				Index:   i,
				Message: fmt.Sprintf("source %q is reserved for dream syntheses", model.DreamSource),
			})
			continue
		}

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
		importance := defaultImportance
		if item.Importance != nil {
			importance = *item.Importance
		}
		mem := &model.Memory{
			ID:          memID,
			NamespaceID: ns.ID,
			Content:     item.Content,
			ContentHash: hash,
			Source:      source,
			Origin:      model.OriginUser,
			Tags:        item.Tags,
			Confidence:  defaultConfidence,
			Importance:  importance,
			Metadata:    item.Metadata,
			CreatedAt:   now,
			UpdatedAt:   now,
			ExpiresAt:   expiresAt,
		}

		// Stage the memory, its ingestion log, and its enrichment job for
		// batched persistence after the loop. Dedup state is recorded now so a
		// later same-hash item in this batch is still collapsed.
		seenHashes[hash] = memID
		pendingMems = append(pendingMems, mem)
		pendingIdx = append(pendingIdx, i)
		pendingLogs = append(pendingLogs, &model.IngestionLog{
			ID:          uuid.New(),
			NamespaceID: ns.ID,
			Source:      item.Source,
			RawContent:  item.Content,
			MemoryIDs:   []uuid.UUID{memID},
			Status:      "completed",
			Metadata:    item.Metadata,
			CreatedAt:   time.Now(),
		})
		pendingJobs = append(pendingJobs, &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    memID,
			NamespaceID: ns.ID,
			Status:      "pending",
			Priority:    0,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		})
	}

	memoriesCreated, errs = s.persistBatch(ctx, pendingMems, pendingLogs, pendingJobs, pendingIdx, errs)

	latency := time.Since(start).Milliseconds()

	return &BatchStoreResponse{
		Processed:       len(req.Items),
		MemoriesCreated: memoriesCreated,
		Errors:          errs,
		LatencyMs:       latency,
	}, nil
}

// persistBatch writes the staged memories, ingestion logs, and enrichment jobs
// after the validation/dedup loop. Memories use the batch fast path when the
// concrete repo supports it (one atomic INSERT per chunk); on any error it
// falls back to per-row Create so a single bad row is attributed to its item
// index rather than failing the whole batch (the batch path is atomic, so the
// fallback cannot collide on a primary key already inserted). Ingestion logs
// and enrichment jobs are fire-and-forget: errors are logged, never surfaced,
// matching the prior per-item behavior. Returns the number of memories created
// and the (possibly extended) error list.
func (s *BatchStoreService) persistBatch(
	ctx context.Context,
	mems []*model.Memory,
	logs []*model.IngestionLog,
	jobs []*model.EnrichmentJob,
	idx []int,
	errs []BatchStoreError,
) (int, []BatchStoreError) {
	if len(mems) == 0 {
		return 0, errs
	}

	created := 0
	batched := false
	if bc, ok := s.memories.(memoryBatchCreator); ok {
		if err := bc.BatchCreate(ctx, mems); err != nil {
			slog.Warn("batch_store: batch memory create failed; falling back to per-row",
				"count", len(mems), "err", err)
		} else {
			batched = true
			created = len(mems)
		}
	}
	if !batched {
		// Per-row fallback with per-item error attribution. Only stage the log
		// and job for rows that actually persisted.
		okLogs := make([]*model.IngestionLog, 0, len(logs))
		okJobs := make([]*model.EnrichmentJob, 0, len(jobs))
		for k, mem := range mems {
			if err := s.memories.Create(ctx, mem); err != nil {
				errs = append(errs, BatchStoreError{
					Index:   idx[k],
					Message: fmt.Sprintf("failed to create memory: %v", err),
				})
				continue
			}
			created++
			okLogs = append(okLogs, logs[k])
			okJobs = append(okJobs, jobs[k])
		}
		logs, jobs = okLogs, okJobs
	}
	if s.metrics != nil {
		for i := 0; i < created; i++ {
			s.metrics.MemoriesTotal.Inc()
		}
	}

	if blc, ok := s.ingestionLogs.(ingestionLogBatchCreator); ok {
		if err := blc.BatchCreate(ctx, logs); err != nil {
			slog.Warn("batch_store: batch ingestion-log create failed", "count", len(logs), "err", err)
		}
	} else {
		for _, lg := range logs {
			_ = s.ingestionLogs.Create(ctx, lg)
		}
	}

	if bqe, ok := s.enrichmentQueue.(enrichmentQueueBatchEnqueuer); ok {
		if err := bqe.BatchEnqueue(ctx, jobs); err != nil {
			slog.Warn("batch_store: batch enrichment enqueue failed", "count", len(jobs), "err", err)
		}
	} else {
		for _, j := range jobs {
			_, _ = s.enrichmentQueue.Enqueue(ctx, j)
		}
	}

	return created, errs
}
