package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// EnrichRequest contains the parameters for an enrich operation.
type EnrichRequest struct {
	ProjectID uuid.UUID   `json:"project_id"`
	MemoryIDs []uuid.UUID `json:"memory_ids,omitempty"` // specific IDs
	All       bool        `json:"all,omitempty"`        // enrich all un-enriched
	Priority  int         `json:"priority,omitempty"`   // default 0
	// IncludeSuperseded enrolls superseded losers in the enrichment pass.
	// Default false skips them so the queue doesn't burn tokens on rows
	// already slated for prune.
	IncludeSuperseded bool `json:"include_superseded,omitempty"`
}

// EnrichResponse contains the result of an enrich operation.
type EnrichResponse struct {
	Queued    int   `json:"queued"`
	Skipped   int   `json:"skipped"` // already enriched
	LatencyMs int64 `json:"latency_ms"`
}

// LineageQuerier provides read-only lineage lookups used by multiple services.
type LineageQuerier interface {
	FindParentIDs(ctx context.Context, namespaceID uuid.UUID, memoryIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error)
	// FindChildIDsByRelation returns the children of a memory restricted to
	// the given lineage relations. Filtered to keep cascades scoped to the
	// caller's intent (e.g. extraction edges only) and to keep self-edges
	// from one relation from feeding cycles into another.
	FindChildIDsByRelation(ctx context.Context, namespaceID uuid.UUID, parentID uuid.UUID, relations []string) ([]uuid.UUID, error)
}

// AugmentationCandidateLister returns the IDs of memories whose stored vector
// pre-dates the query-augmentation flag flip (augmented_embedding_at IS NULL).
// Kept as a tiny interface so the backfill code path can be wired without
// touching the broad MemoryReader interface, which has many implementors.
type AugmentationCandidateLister interface {
	ListAugmentationBackfillCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]storage.BackfillCandidate, error)
}

// ParaphraseCandidateLister returns the IDs of enriched parent memories with
// at least one live extracted-fact lineage child. Used by
// BackfillExtractedFactParaphrase to enumerate parents whose existing
// children should be swept for paraphrase suppression. Same tiny-interface
// pattern as AugmentationCandidateLister.
type ParaphraseCandidateLister interface {
	ListEnrichedParentsWithExtractedChildren(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]storage.BackfillCandidate, error)
}

// EnrichService orchestrates bulk enrichment queueing for memories in a project.
type EnrichService struct {
	memories         MemoryReader
	projects         ProjectRepository
	enrichmentQueue  EnrichmentQueueRepository
	lineage          LineageQuerier
	augLister        AugmentationCandidateLister
	paraphraseLister ParaphraseCandidateLister
}

// NewEnrichService creates a new EnrichService with the given dependencies.
func NewEnrichService(
	memories MemoryReader,
	projects ProjectRepository,
	enrichmentQueue EnrichmentQueueRepository,
	lineage LineageQuerier,
) *EnrichService {
	return &EnrichService{
		memories:        memories,
		projects:        projects,
		enrichmentQueue: enrichmentQueue,
		lineage:         lineage,
	}
}

// AttachAugmentationLister wires the candidate lister used by
// BackfillAugmentation. Optional: when nil, BackfillAugmentation returns an
// explanatory error rather than silently no-oping.
func (s *EnrichService) AttachAugmentationLister(lister AugmentationCandidateLister) {
	s.augLister = lister
}

// AttachParaphraseCandidateLister wires the candidate lister used by
// BackfillExtractedFactParaphrase.
func (s *EnrichService) AttachParaphraseCandidateLister(lister ParaphraseCandidateLister) {
	s.paraphraseLister = lister
}

// BackfillExtractedFactParaphraseRequest scopes a paraphrase-guard backfill
// to a project (or the whole deployment if ProjectID is zero). DryRun returns
// the candidate parent count without enqueueing any jobs.
type BackfillExtractedFactParaphraseRequest struct {
	ProjectID uuid.UUID `json:"project_id,omitempty"`
	DryRun    bool      `json:"dry_run,omitempty"`
	// Limit caps the number of candidate parents enqueued in one call (0 =
	// no cap). Operators with large namespaces (>5k memories) typically
	// invoke this in pages of SettingExtractedFactBackfillBatchSize and
	// re-run until CandidateCount returns 0.
	Limit int `json:"limit,omitempty"`
}

// BackfillExtractedFactParaphraseResponse reports the outcome of one call.
type BackfillExtractedFactParaphraseResponse struct {
	CandidateCount int   `json:"candidate_count"`
	Enqueued       int   `json:"enqueued"`
	DryRun         bool  `json:"dry_run"`
	LatencyMs      int64 `json:"latency_ms"`
}

// BackfillExtractedFactParaphrase enumerates parents with extracted-fact
// children and enqueues one paraphrase-guard sweep job per parent onto the
// shared enrichment_queue. Each job carries the JobMarkerOnlyParaphraseGuard
// sentinel in StepsCompleted; the worker routes ONLY to the sweep handler
// for these jobs, skipping fact/entity extraction, augmentation, and embed.
// Progress is observable through the standard queue-admin endpoints.
func (s *EnrichService) BackfillExtractedFactParaphrase(ctx context.Context, req *BackfillExtractedFactParaphraseRequest) (*BackfillExtractedFactParaphraseResponse, error) {
	start := time.Now()
	if s.paraphraseLister == nil {
		return nil, fmt.Errorf("paraphrase candidate lister not configured (call AttachParaphraseCandidateLister)")
	}

	var namespaceIDs []uuid.UUID
	if req.ProjectID != uuid.Nil {
		project, err := s.projects.GetByID(ctx, req.ProjectID)
		if err != nil {
			return nil, fmt.Errorf("project not found: %w", err)
		}
		namespaceIDs = []uuid.UUID{project.NamespaceID}
	}

	candidates, err := s.paraphraseLister.ListEnrichedParentsWithExtractedChildren(ctx, namespaceIDs, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("list paraphrase backfill candidates: %w", err)
	}

	resp := &BackfillExtractedFactParaphraseResponse{
		CandidateCount: len(candidates),
		DryRun:         req.DryRun,
	}
	if req.DryRun || len(candidates) == 0 {
		resp.LatencyMs = time.Since(start).Milliseconds()
		return resp, nil
	}

	markerBytes, err := json.Marshal([]string{model.JobMarkerOnlyParaphraseGuard})
	if err != nil {
		return nil, fmt.Errorf("marshal job marker: %w", err)
	}

	now := time.Now()
	for _, cand := range candidates {
		job := &model.EnrichmentJob{
			ID:             uuid.New(),
			MemoryID:       cand.ID,
			NamespaceID:    cand.NamespaceID,
			Status:         model.EnrichmentStatusPending,
			Priority:       0,
			Attempts:       0,
			MaxAttempts:    3,
			StepsCompleted: markerBytes,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		inserted, err := s.enrichmentQueue.Enqueue(ctx, job)
		if err != nil {
			return nil, fmt.Errorf("enqueue paraphrase backfill for memory %s: %w", cand.ID, err)
		}
		if inserted {
			resp.Enqueued++
		}
	}
	resp.LatencyMs = time.Since(start).Milliseconds()
	return resp, nil
}

// BackfillAugmentationRequest scopes a query-augmentation backfill. ProjectID
// == uuid.Nil scans the entire deployment (admin-only path).
type BackfillAugmentationRequest struct {
	ProjectID uuid.UUID `json:"project_id,omitempty"`
	DryRun    bool      `json:"dry_run,omitempty"`
	// Limit caps the number of candidates enqueued in one call. 0 = no cap.
	// Useful when the operator wants to feed the queue gradually rather than
	// flooding it with millions of jobs at once.
	Limit int `json:"limit,omitempty"`
}

// BackfillAugmentationResponse reports the outcome of one backfill call.
// CandidateCount is the size of the candidate set the lister returned;
// Enqueued is the number of jobs actually inserted into the queue (0 when
// DryRun=true).
type BackfillAugmentationResponse struct {
	CandidateCount int   `json:"candidate_count"`
	Enqueued       int   `json:"enqueued"`
	DryRun         bool  `json:"dry_run"`
	LatencyMs      int64 `json:"latency_ms"`
}

// BackfillAugmentation enqueues enrichment jobs for memories whose vector was
// written before the query-augmentation feature was enabled. Distinct from
// Enrich: this path INCLUDES already-enriched rows because re-embedding with
// augmentation is exactly the point. The worker's per-step idempotency means
// fact and entity extraction are skipped automatically for already-enriched
// rows, so the cost is one extra LLM augmentation call plus one embed call
// per memory.
func (s *EnrichService) BackfillAugmentation(ctx context.Context, req *BackfillAugmentationRequest) (*BackfillAugmentationResponse, error) {
	start := time.Now()
	if s.augLister == nil {
		return nil, fmt.Errorf("augmentation candidate lister not configured (call AttachAugmentationLister)")
	}

	var namespaceIDs []uuid.UUID
	if req.ProjectID != uuid.Nil {
		project, err := s.projects.GetByID(ctx, req.ProjectID)
		if err != nil {
			return nil, fmt.Errorf("project not found: %w", err)
		}
		namespaceIDs = []uuid.UUID{project.NamespaceID}
	}

	candidates, err := s.augLister.ListAugmentationBackfillCandidates(ctx, namespaceIDs, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("list backfill candidates: %w", err)
	}

	resp := &BackfillAugmentationResponse{
		CandidateCount: len(candidates),
		DryRun:         req.DryRun,
	}
	if req.DryRun || len(candidates) == 0 {
		resp.LatencyMs = time.Since(start).Milliseconds()
		return resp, nil
	}

	// The lister returns each candidate's namespace, so the job can be
	// enqueued directly without a per-id read (which would have to cross
	// namespaces for the whole-deployment admin sweep).
	now := time.Now()
	for _, cand := range candidates {
		job := &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    cand.ID,
			NamespaceID: cand.NamespaceID,
			Status:      "pending",
			Priority:    0,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		inserted, err := s.enrichmentQueue.Enqueue(ctx, job)
		if err != nil {
			return nil, fmt.Errorf("enqueue augmentation backfill for memory %s: %w", cand.ID, err)
		}
		if inserted {
			resp.Enqueued++
		}
	}
	resp.LatencyMs = time.Since(start).Milliseconds()
	return resp, nil
}

// Enrich enqueues enrichment jobs for the specified memories or all un-enriched
// memories in the project's namespace. Superseded rows are excluded by default;
// set req.IncludeSuperseded to enroll them.
func (s *EnrichService) Enrich(ctx context.Context, req *EnrichRequest) (*EnrichResponse, error) {
	start := time.Now()

	// Validate required fields.
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}
	if len(req.MemoryIDs) == 0 && !req.All {
		return nil, fmt.Errorf("at least one of memory_ids or all must be specified")
	}

	// Look up project.
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	namespaceID := project.NamespaceID

	var memories []model.Memory

	if len(req.MemoryIDs) > 0 {
		// Fetch specific memories, bounded to the project's namespace by the
		// query itself (GetBatch drops ids outside the namespace).
		batch, err := s.memories.GetBatch(ctx, req.MemoryIDs, []uuid.UUID{namespaceID})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch memories: %w", err)
		}
		for _, mem := range batch {
			if mem.SupersededBy != nil && !req.IncludeSuperseded {
				continue
			}
			memories = append(memories, mem)
		}
	} else {
		// Paginate through all memories in the namespace, pushing the
		// supersede filter into SQL so the queue doesn't waste round-trips
		// shipping rows we'd skip in Go.
		const pageSize = 100
		filters := storage.MemoryListFilters{HideSuperseded: !req.IncludeSuperseded}
		offset := 0
		for {
			page, err := s.memories.ListByNamespaceFiltered(ctx, namespaceID, filters, pageSize, offset)
			if err != nil {
				return nil, fmt.Errorf("failed to list memories: %w", err)
			}
			memories = append(memories, page...)
			if len(page) < pageSize {
				break
			}
			offset += pageSize
		}
	}

	// Batch-lookup which memories are children to skip them.
	childSet := make(map[uuid.UUID]bool)
	if s.lineage != nil {
		ids := make([]uuid.UUID, len(memories))
		for i := range memories {
			ids[i] = memories[i].ID
		}
		if parentMap, err := s.lineage.FindParentIDs(ctx, namespaceID, ids); err == nil {
			for childID := range parentMap {
				childSet[childID] = true
			}
		}
	}

	// Enqueue un-enriched, non-child memories.
	queued := 0
	skipped := 0
	now := time.Now()

	for i := range memories {
		mem := &memories[i]
		if mem.Enriched || childSet[mem.ID] {
			skipped++
			continue
		}

		job := &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    mem.ID,
			NamespaceID: namespaceID,
			Status:      "pending",
			Priority:    req.Priority,
			Attempts:    0,
			MaxAttempts: 3,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		inserted, err := s.enrichmentQueue.Enqueue(ctx, job)
		if err != nil {
			return nil, fmt.Errorf("failed to enqueue enrichment job for memory %s: %w", mem.ID, err)
		}
		if inserted {
			queued++
		}
	}

	latency := time.Since(start).Milliseconds()

	return &EnrichResponse{
		Queued:    queued,
		Skipped:   skipped,
		LatencyMs: latency,
	}, nil
}
