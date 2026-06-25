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

// MultiVectorCandidateLister returns live memories to re-facet for the
// multi-vector backfill. Same tiny-interface pattern as the others.
type MultiVectorCandidateLister interface {
	ListMultiVectorBackfillCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]storage.BackfillCandidate, error)
}

// MissingEmbeddingCandidateLister returns live, embeddable memories with no
// stored vector (embedding_dim IS NULL) — the embedding-stranded set. Used by
// BackfillMissingEmbeddings to re-enqueue them for re-embedding. Same
// tiny-interface pattern as the others.
type MissingEmbeddingCandidateLister interface {
	ListMissingEmbeddingCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]storage.BackfillCandidate, error)
}

// DreamEntityCandidateLister returns active consolidation dreams lacking
// entity-graph coverage (origin=dream, live, non-empty source_memory_ids, no
// sourced relationship). Used by BackfillConsolidationEntities to enqueue
// entity-only jobs that recover graph coverage stranded before dreams were
// extracted. Same tiny-interface pattern as the others.
type DreamEntityCandidateLister interface {
	ListDreamEntityBackfillCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]storage.BackfillCandidate, error)
}

// ReExtractStore is the storage surface the re-extraction path needs beyond the
// shared queue/lineage deps: candidate listing, enriched reset, and soft-delete
// of prior extracted-fact children. Kept as a tiny interface so re-extraction
// can be wired without widening MemoryReader.
type ReExtractStore interface {
	ListReExtractCandidates(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]storage.BackfillCandidate, error)
	ResetEnriched(ctx context.Context, id, namespaceID uuid.UUID) error
	SoftDelete(ctx context.Context, id, namespaceID uuid.UUID) error
}

// EnrichService orchestrates bulk enrichment queueing for memories in a project.
type EnrichService struct {
	memories          MemoryReader
	projects          ProjectRepository
	enrichmentQueue   EnrichmentQueueRepository
	lineage           LineageQuerier
	augLister         AugmentationCandidateLister
	paraphraseLister  ParaphraseCandidateLister
	mvLister          MultiVectorCandidateLister
	missingEmbLister  MissingEmbeddingCandidateLister
	dreamEntityLister DreamEntityCandidateLister
	reExtractStore    ReExtractStore
	graphReaper       GraphReaper
}

// AttachReExtract wires the dependencies for ReExtract: the candidate/reset/
// soft-delete store and the graph reaper that tombstones a memory's footprint.
// Optional: when unset, ReExtract returns an explanatory error.
func (s *EnrichService) AttachReExtract(store ReExtractStore, reaper GraphReaper) {
	s.reExtractStore = store
	s.graphReaper = reaper
}

// ReExtractRequest scopes a full re-extraction to a project (or the whole
// deployment when ProjectID is zero). DryRun returns the candidate count and
// writes nothing. Limit caps how many memories one call processes (0 = no cap);
// re-extraction over a large deployment is run in pages.
type ReExtractRequest struct {
	ProjectID uuid.UUID `json:"project_id,omitempty"`
	DryRun    bool      `json:"dry_run,omitempty"`
	Limit     int       `json:"limit,omitempty"`
}

// ReExtractResponse reports the outcome of one re-extraction call.
type ReExtractResponse struct {
	CandidateCount      int   `json:"candidate_count"`
	Enqueued            int   `json:"enqueued"`
	DryRun              bool  `json:"dry_run"`
	EntitiesRecomputed  int   `json:"entities_recomputed"`
	FactChildrenRemoved int   `json:"fact_children_removed"`
	LatencyMs           int64 `json:"latency_ms"`
}

// ReExtract re-runs fact and entity extraction over already-enriched memories
// under the current prompt and vocabulary. For each candidate it tombstones the
// memory's prior graph footprint (so it is not double-counted), removes prior
// extracted-fact children (so they are not duplicated), clears the enriched flag
// and re-enqueues the memory. The worker's skip guard gates on enriched AND on
// the relationships-exist probe, so both the reset and the tombstone are
// required for extraction to actually re-run. Dream syntheses and procedural
// entries are never candidates. DryRun returns the candidate count only.
func (s *EnrichService) ReExtract(ctx context.Context, req *ReExtractRequest) (*ReExtractResponse, error) {
	start := time.Now()
	if s.reExtractStore == nil || s.graphReaper == nil {
		return nil, fmt.Errorf("re-extraction not configured (call AttachReExtract)")
	}

	var namespaceIDs []uuid.UUID
	if req.ProjectID != uuid.Nil {
		project, perr := s.projects.GetByID(ctx, req.ProjectID)
		if perr != nil {
			return nil, fmt.Errorf("project not found: %w", perr)
		}
		namespaceIDs = []uuid.UUID{project.NamespaceID}
	}

	candidates, err := s.reExtractStore.ListReExtractCandidates(ctx, namespaceIDs, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("list re-extract candidates: %w", err)
	}
	resp := &ReExtractResponse{CandidateCount: len(candidates), DryRun: req.DryRun}
	if req.DryRun || len(candidates) == 0 {
		resp.LatencyMs = time.Since(start).Milliseconds()
		return resp, nil
	}

	now := time.Now()
	for _, cand := range candidates {
		affected, rerr := s.graphReaper.ReapMemoryFootprint(ctx, cand.NamespaceID, cand.ID)
		if rerr != nil {
			return nil, fmt.Errorf("reap footprint for memory %s: %w", cand.ID, rerr)
		}
		resp.EntitiesRecomputed += affected

		if children, lerr := s.lineage.FindChildIDsByRelation(ctx, cand.NamespaceID, cand.ID, storage.ExtractedChildRelations); lerr == nil {
			for _, child := range children {
				if derr := s.reExtractStore.SoftDelete(ctx, child, cand.NamespaceID); derr == nil {
					resp.FactChildrenRemoved++
				}
			}
		}

		if err := s.reExtractStore.ResetEnriched(ctx, cand.ID, cand.NamespaceID); err != nil {
			return nil, fmt.Errorf("reset enriched for memory %s: %w", cand.ID, err)
		}

		job := &model.EnrichmentJob{
			ID:          uuid.New(),
			MemoryID:    cand.ID,
			NamespaceID: cand.NamespaceID,
			Status:      "pending",
			MaxAttempts: 3,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		inserted, ierr := s.enrichmentQueue.Enqueue(ctx, job)
		if ierr != nil {
			return nil, fmt.Errorf("enqueue re-extract for memory %s: %w", cand.ID, ierr)
		}
		if inserted {
			resp.Enqueued++
		}
	}
	resp.LatencyMs = time.Since(start).Milliseconds()
	return resp, nil
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

// AttachMultiVectorLister wires the candidate lister used by BackfillMultiVector.
// Optional: when nil, BackfillMultiVector returns an explanatory error.
func (s *EnrichService) AttachMultiVectorLister(lister MultiVectorCandidateLister) {
	s.mvLister = lister
}

// AttachDreamEntityLister wires the candidate lister used by
// BackfillConsolidationEntities.
func (s *EnrichService) AttachDreamEntityLister(lister DreamEntityCandidateLister) {
	s.dreamEntityLister = lister
}

// AttachMissingEmbeddingLister wires the candidate lister used by
// BackfillMissingEmbeddings. Optional: when nil, BackfillMissingEmbeddings
// returns an explanatory error.
func (s *EnrichService) AttachMissingEmbeddingLister(lister MissingEmbeddingCandidateLister) {
	s.missingEmbLister = lister
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
	count, enq, err := s.runBackfill(ctx, req.ProjectID, req.DryRun, req.Limit, model.JobMarkerOnlyParaphraseGuard, s.paraphraseLister.ListEnrichedParentsWithExtractedChildren)
	if err != nil {
		return nil, err
	}
	return &BackfillExtractedFactParaphraseResponse{
		CandidateCount: count,
		Enqueued:       enq,
		DryRun:         req.DryRun,
		LatencyMs:      time.Since(start).Milliseconds(),
	}, nil
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
// BackfillMultiVectorRequest scopes the multi-vector (facet) backfill. ProjectID
// is optional; omit to sweep the whole deployment. Limit caps candidates per call.
type BackfillMultiVectorRequest struct {
	ProjectID uuid.UUID `json:"project_id,omitempty"`
	DryRun    bool      `json:"dry_run,omitempty"`
	Limit     int       `json:"limit,omitempty"`
}

// BackfillMultiVectorResponse reports the backfill outcome.
type BackfillMultiVectorResponse struct {
	CandidateCount int   `json:"candidate_count"`
	Enqueued       int   `json:"enqueued"`
	DryRun         bool  `json:"dry_run"`
	LatencyMs      int64 `json:"latency_ms"`
}

// BackfillConsolidationEntitiesRequest scopes the consolidation-entity backfill.
// ProjectID is optional; omit to sweep the whole deployment. Limit caps
// candidates per call.
type BackfillConsolidationEntitiesRequest struct {
	ProjectID uuid.UUID `json:"project_id,omitempty"`
	DryRun    bool      `json:"dry_run,omitempty"`
	Limit     int       `json:"limit,omitempty"`
}

// BackfillConsolidationEntitiesResponse reports the backfill outcome.
type BackfillConsolidationEntitiesResponse struct {
	CandidateCount int   `json:"candidate_count"`
	Enqueued       int   `json:"enqueued"`
	DryRun         bool  `json:"dry_run"`
	LatencyMs      int64 `json:"latency_ms"`
}

// BackfillMissingEmbeddingsRequest scopes a missing-embedding repair to a project
// (or the whole deployment if ProjectID is zero). DryRun returns the candidate
// count without enqueueing. Limit caps the batch (0 = no cap).
type BackfillMissingEmbeddingsRequest struct {
	ProjectID uuid.UUID `json:"project_id,omitempty"`
	DryRun    bool      `json:"dry_run,omitempty"`
	Limit     int       `json:"limit,omitempty"`
}

// BackfillMissingEmbeddingsResponse reports the outcome of one call.
type BackfillMissingEmbeddingsResponse struct {
	CandidateCount int   `json:"candidate_count"`
	Enqueued       int   `json:"enqueued"`
	DryRun         bool  `json:"dry_run"`
	LatencyMs      int64 `json:"latency_ms"`
}

// BackfillMultiVector enqueues live memories for a lean facet-only sweep: each
// job carries the JobMarkerOnlyMultiVector sentinel, so the worker reuses the
// memory's stored facet-0 vector and runs only the per-topic sentence embeds
// (gated by enrichment.multi_vector.enabled). No ingestion-decision, no
// query-augmentation LLM call, and no whole-memory re-embed. This is the path
// that recovers facets for memories stored before the feature was enabled,
// including high-confidence syntheses that already superseded their sources.
// Idempotent: re-faceting replaces the facet set.
// candidateLister lists backfill candidates (id+namespace) for a project scope.
type candidateLister func(ctx context.Context, namespaceIDs []uuid.UUID, limit int) ([]storage.BackfillCandidate, error)

// runBackfill resolves the optional project scope to a namespace filter, lists
// candidates, and (unless dryRun) enqueues a re-enrichment job per candidate.
// Shared by every backfill (augmentation, multi-vector, missing-embeddings,
// extracted-fact paraphrase, consolidation-entities). When marker is non-empty
// it is stamped as the job's sole StepsCompleted entry. A JobMarkerOnly*
// sentinel routes the worker to a lean no-LLM handler (the multi-vector facet
// sweep or the paraphrase-guard sweep); a plain step name (e.g. fact_extraction
// for the consolidation-entities backfill) instead skips that one step and runs
// the normal pipeline. An empty marker enqueues a full-pipeline job (the
// augmentation / missing-embeddings paths, which want the re-embed). The
// candidate's namespace rides along so the enqueue needs no per-id read on the
// whole-deployment sweep.
func (s *EnrichService) runBackfill(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int, marker string, list candidateLister) (candidateCount, enqueued int, err error) {
	var namespaceIDs []uuid.UUID
	if projectID != uuid.Nil {
		project, perr := s.projects.GetByID(ctx, projectID)
		if perr != nil {
			return 0, 0, fmt.Errorf("project not found: %w", perr)
		}
		namespaceIDs = []uuid.UUID{project.NamespaceID}
	}

	candidates, err := list(ctx, namespaceIDs, limit)
	if err != nil {
		return 0, 0, err
	}
	if dryRun || len(candidates) == 0 {
		return len(candidates), 0, nil
	}

	var markerBytes json.RawMessage
	if marker != "" {
		markerBytes, err = json.Marshal([]string{marker})
		if err != nil {
			return 0, 0, fmt.Errorf("marshal job marker: %w", err)
		}
	}

	now := time.Now()
	for _, cand := range candidates {
		job := &model.EnrichmentJob{
			ID:             uuid.New(),
			MemoryID:       cand.ID,
			NamespaceID:    cand.NamespaceID,
			Status:         "pending",
			Priority:       0,
			Attempts:       0,
			MaxAttempts:    3,
			StepsCompleted: markerBytes,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		inserted, ierr := s.enrichmentQueue.Enqueue(ctx, job)
		if ierr != nil {
			return 0, 0, fmt.Errorf("enqueue backfill for memory %s: %w", cand.ID, ierr)
		}
		if inserted {
			enqueued++
		}
	}
	return len(candidates), enqueued, nil
}

func (s *EnrichService) BackfillMultiVector(ctx context.Context, req *BackfillMultiVectorRequest) (*BackfillMultiVectorResponse, error) {
	start := time.Now()
	if s.mvLister == nil {
		return nil, fmt.Errorf("multi-vector candidate lister not configured (call AttachMultiVectorLister)")
	}
	count, enq, err := s.runBackfill(ctx, req.ProjectID, req.DryRun, req.Limit, model.JobMarkerOnlyMultiVector, s.mvLister.ListMultiVectorBackfillCandidates)
	if err != nil {
		return nil, err
	}
	return &BackfillMultiVectorResponse{
		CandidateCount: count,
		Enqueued:       enq,
		DryRun:         req.DryRun,
		LatencyMs:      time.Since(start).Milliseconds(),
	}, nil
}

// BackfillConsolidationEntities enqueues an entity-only enrichment job for every
// active consolidation dream that still lacks entity-graph coverage, recovering
// the coverage stranded before dreams were extracted (the
// consolidation-erases-coverage fix). Each job pre-stamps fact_extraction so no
// extracted_fact child memories spawn (also hard-off via the worker's isDream
// clause), while entity extraction runs for the consolidation synthesis (graph
// rows only, never memories). The on-demand counterpart to the
// ConsolidationEntityBackfill dream phase; both select the same candidates and
// enqueue the same job shape. Enqueue dedups against the pending-job unique
// index, and a dream drops out of the candidate set once it has a sourced
// relationship (see ListDreamEntityBackfillCandidates for the one edge — an
// entity-only synthesis — where that gate does not converge).
func (s *EnrichService) BackfillConsolidationEntities(ctx context.Context, req *BackfillConsolidationEntitiesRequest) (*BackfillConsolidationEntitiesResponse, error) {
	start := time.Now()
	if s.dreamEntityLister == nil {
		return nil, fmt.Errorf("dream-entity candidate lister not configured (call AttachDreamEntityLister)")
	}
	count, enq, err := s.runBackfill(ctx, req.ProjectID, req.DryRun, req.Limit, model.StepFactExtraction, s.dreamEntityLister.ListDreamEntityBackfillCandidates)
	if err != nil {
		return nil, err
	}
	return &BackfillConsolidationEntitiesResponse{
		CandidateCount: count,
		Enqueued:       enq,
		DryRun:         req.DryRun,
		LatencyMs:      time.Since(start).Milliseconds(),
	}, nil
}

// BackfillMissingEmbeddings re-enqueues every embedding-stranded memory (no
// stored vector) so the worker re-embeds and finalizes it, restoring vector
// recall. It enqueues a NORMAL enrichment job (empty marker): an already-enriched
// memory skips re-extraction via the worker's mem.Enriched gate and proceeds
// straight to embed + finalize. Runs entirely off the queue, independent of
// dreaming, so an operator with dreaming disabled can still repair strays.
func (s *EnrichService) BackfillMissingEmbeddings(ctx context.Context, req *BackfillMissingEmbeddingsRequest) (*BackfillMissingEmbeddingsResponse, error) {
	start := time.Now()
	if s.missingEmbLister == nil {
		return nil, fmt.Errorf("missing-embedding candidate lister not configured (call AttachMissingEmbeddingLister)")
	}
	count, enq, err := s.runBackfill(ctx, req.ProjectID, req.DryRun, req.Limit, "", s.missingEmbLister.ListMissingEmbeddingCandidates)
	if err != nil {
		return nil, err
	}
	return &BackfillMissingEmbeddingsResponse{
		CandidateCount: count,
		Enqueued:       enq,
		DryRun:         req.DryRun,
		LatencyMs:      time.Since(start).Milliseconds(),
	}, nil
}

func (s *EnrichService) BackfillAugmentation(ctx context.Context, req *BackfillAugmentationRequest) (*BackfillAugmentationResponse, error) {
	start := time.Now()
	if s.augLister == nil {
		return nil, fmt.Errorf("augmentation candidate lister not configured (call AttachAugmentationLister)")
	}
	count, enq, err := s.runBackfill(ctx, req.ProjectID, req.DryRun, req.Limit, "", s.augLister.ListAugmentationBackfillCandidates)
	if err != nil {
		return nil, err
	}
	return &BackfillAugmentationResponse{
		CandidateCount: count,
		Enqueued:       enq,
		DryRun:         req.DryRun,
		LatencyMs:      time.Since(start).Milliseconds(),
	}, nil
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
