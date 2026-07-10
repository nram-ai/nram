package dreaming

import (
	"context"
	"errors"
	"log/slog"
	"sort"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// Sentinel errors for the split embed/persist backfill path: an unavailable
// embedder (no embed_errors penalty, the row is just cleared) versus a real
// embed failure (counts toward embed_errors), mirroring the original
// tryRepair accounting.
var (
	errBackfillNoEmbedder     = errors.New("dreaming: embedding backfill embedder unavailable")
	errBackfillEmptyEmbedding = errors.New("dreaming: embedding backfill produced no vector")
)

// backfillDims returns the supported memory vector dimensions in
// ascending order so per-cycle scans visit them deterministically.
// Map iteration would otherwise pick a random order and the cap could
// drain on different dims across cycles, making telemetry harder to
// reason about.
func backfillDims() []int {
	dims := make([]int, 0, len(storage.SupportedVectorDimensions))
	for d := range storage.SupportedVectorDimensions {
		dims = append(dims, d)
	}
	sort.Ints(dims)
	return dims
}

// EmbeddingBackfillPhase repairs two vector/row divergences. First, rows whose
// embedding_dim is set but whose memory_vectors_<dim> row is missing: each is
// re-embedded (fresh vector) or has embedding_dim cleared so the row matches the
// vector store. Second, rows whose embedding_dim is NULL (the embed never
// recorded a dim): if a vector survived at some supported dim the dim is
// restamped with no re-embed (desync), otherwise the row is re-embedded; demoted
// rows stay vectorless. Runs before paraphrase dedup so the downstream phase
// sees the repaired state in the same cycle, and before the multi-vector facet
// backfill phase so restored embedding_dims become facet candidates same-cycle.
type EmbeddingBackfillPhase struct {
	repairer    MemoryDimRepairer
	memWriter   MemoryWriter
	vectorStore storage.VectorStore
	embedder    EmbeddingProviderFunc
	settings    SettingsResolver
}

// NewEmbeddingBackfillPhase constructs the phase. embedder may be nil; in
// that case every divergent row is repaired by clearing embedding_dim.
func NewEmbeddingBackfillPhase(
	repairer MemoryDimRepairer,
	memWriter MemoryWriter,
	vectorStore storage.VectorStore,
	embedder EmbeddingProviderFunc,
	settings SettingsResolver,
) *EmbeddingBackfillPhase {
	return &EmbeddingBackfillPhase{
		repairer:    repairer,
		memWriter:   memWriter,
		vectorStore: vectorStore,
		embedder:    embedder,
		settings:    settings,
	}
}

// Name returns the phase identifier.
func (p *EmbeddingBackfillPhase) Name() string { return model.DreamPhaseEmbeddingBackfill }

// Execute scans every supported dim's missing-vector rows up to the
// per-cycle cap, repairs or clears each, and reports residual when more
// rows are pending than the cap allowed.
func (p *EmbeddingBackfillPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	if p.settings != nil && !p.settings.ResolveBool(ctx, service.SettingDreamEmbeddingBackfillEnabled, "global") {
		return PhaseResult{}, nil
	}
	if p.repairer == nil || p.memWriter == nil || p.vectorStore == nil {
		return PhaseResult{}, nil
	}

	cap, _ := p.settings.ResolveInt(ctx, service.SettingDreamEmbeddingBackfillCapPerCycle, "global")
	if cap <= 0 {
		cap = 200
	}

	stats := map[string]any{
		"sub_phase":     model.DreamPhaseEmbeddingBackfill,
		"candidates":    0,
		"visited":       0,
		"repaired":      0,
		"cleared":       0,
		"embed_errors":  0,
		"upsert_errors": 0,
		"update_errors": 0,
		"per_cycle_cap": cap,
	}
	tokensBefore := 0
	if budget != nil {
		tokensBefore = budget.Used()
	}

	foundTotal := 0
	visited := 0
	progressStep := progressEmitStep(cap)
	concurrency := max(p.settings.ResolveIntWithDefault(ctx, service.SettingDreamLLMConcurrency, "global"), 1)

	// Iterate every supported memory dim in ascending order. The find
	// query is per-dim because the LEFT JOIN targets a single
	// memory_vectors_<dim> table (PostgreSQL) or a single dimension
	// filter (SQLite shared table).
	for _, dim := range backfillDims() {
		remaining := cap - visited
		if remaining <= 0 {
			break
		}
		// Probe one row beyond `remaining` so the residual signal can
		// distinguish "all clean" from "cap reached with more pending."
		rows, err := p.repairer.FindMemoriesMissingVector(ctx, cycle.NamespaceID, dim, remaining+1)
		if err != nil {
			slog.Warn("dreaming: embedding backfill find failed",
				"cycle", cycle.ID, "namespace", cycle.NamespaceID, "dim", dim, "err", err)
			continue
		}
		foundTotal += len(rows)
		toProcess := rows
		if len(toProcess) > remaining {
			toProcess = toProcess[:remaining]
		}
		// Re-embed all candidates in parallel (the embed call is the slow part;
		// runBounded caps in-flight calls to concurrency), then persist each
		// result serially in order so the vector writes and stats accounting
		// stay deterministic. Backfill has no per-item budget gate, so unlike
		// the LLM phases it needs no windowing.
		vecs := make([][]float32, len(toProcess))
		errs := make([]error, len(toProcess))
		runBounded(concurrency, len(toProcess), func(k int) {
			m := toProcess[k]
			vecs[k], errs[k] = p.embedForBackfill(ctx, &m, dim, budget)
		})
		for k := range toProcess {
			visited++
			if shouldEmitProgress(visited-1, cap, progressStep) {
				slog.Debug("dreaming: embedding backfill progress",
					"cycle", cycle.ID, "dim", dim,
					"memory", visited, "of", cap)
			}
			mem := toProcess[k]
			if p.tryRepair(ctx, &mem, vecs[k], errs[k], stats) {
				continue
			}
			// Only clear the stale embedding_dim when the embedder is genuinely
			// unavailable (nothing can re-embed this row now). A TRANSIENT embed
			// failure — a provider error, an outage, an empty response — must NOT
			// null the dim: with a crash-looping embedder that would strand
			// divergent rows en masse (exactly the failure mode that produced the
			// embedding-stranded backlog). Leaving the dim keeps the row a
			// FindMemoriesMissingVector candidate the next cycle retries.
			if errors.Is(errs[k], errBackfillNoEmbedder) {
				p.clearDim(ctx, &mem, stats)
			}
		}
	}

	// Null-dim repair shares the same per-cycle cap as the per-dim scan above.
	nullFound, nullVisited := p.repairNullDimRows(ctx, cycle, budget, cap-visited, concurrency, stats)
	foundTotal += nullFound
	visited += nullVisited

	stats["candidates"] = foundTotal
	stats["visited"] = visited
	p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)

	if foundTotal > visited {
		return PhaseResult{
			HasResidual:    true,
			ResidualReason: ResidualReasonMoreCandidatesThanBatch,
			ResidualDetail: map[string]any{
				"candidates":    foundTotal,
				"visited":       visited,
				"per_cycle_cap": cap,
			},
		}, nil
	}
	return PhaseResult{}, nil
}

// repairNullDimRows heals memories whose embedding_dim is NULL, which the
// per-dim FindMemoriesMissingVector scan cannot see (it matches embedding_dim =
// dim). These arise when the enrichment embed produced no vector yet the job
// still finalized (now guarded at the write path in worker.finalizeJob, but
// legacy rows persist). Within the supplied remaining cap: if a stored vector
// still survives at some supported dim (embedding_dim was lost while the vector
// remained = desync), embedding_dim is restamped with no re-embed; otherwise the
// row is re-embedded. Returns how many candidates were found and processed so
// the caller can fold them into the cycle's running totals.
func (p *EmbeddingBackfillPhase) repairNullDimRows(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, remaining, concurrency int, stats map[string]any) (found, processed int) {
	if remaining <= 0 {
		return 0, 0
	}
	nullRows, err := p.repairer.FindMemoriesNullEmbeddingDim(ctx, cycle.NamespaceID, remaining+1)
	if err != nil {
		slog.Warn("dreaming: embedding backfill null-dim find failed",
			"cycle", cycle.ID, "namespace", cycle.NamespaceID, "err", err)
		return 0, 0
	}
	found = len(nullRows)
	toProcess := nullRows
	if len(toProcess) > remaining {
		toProcess = toProcess[:remaining]
	}
	if len(toProcess) == 0 {
		return found, 0
	}

	existingDim := p.probeExistingVectors(ctx, cycle, toProcess)

	// Re-embed only the genuinely vectorless rows (desync rows already have a
	// vector). Embed in parallel, then persist serially in order.
	embedDim := p.bestEmbedDim()
	vecs := make([][]float32, len(toProcess))
	errs := make([]error, len(toProcess))
	needEmbed := make([]int, 0, len(toProcess))
	for i := range toProcess {
		if _, ok := existingDim[toProcess[i].ID]; !ok {
			needEmbed = append(needEmbed, i)
		}
	}
	runBounded(concurrency, len(needEmbed), func(k int) {
		i := needEmbed[k]
		m := toProcess[i]
		vecs[i], errs[i] = p.embedForBackfill(ctx, &m, embedDim, budget)
	})

	for i := range toProcess {
		processed++
		mem := toProcess[i]
		if d, ok := existingDim[mem.ID]; ok {
			// Desync: the vector survived; restamp embedding_dim only.
			mem.EmbeddingDim = &d
			if err := p.memWriter.UpdateEmbeddingDim(ctx, mem.ID, d); err != nil {
				slog.Warn("dreaming: embedding backfill null-dim restamp failed",
					"memory", mem.ID, "dim", d, "err", err)
				stats["update_errors"] = stats["update_errors"].(int) + 1
				continue
			}
			stats["repaired"] = stats["repaired"].(int) + 1
			continue
		}
		// No surviving vector: a fresh embed writes the vector and stamps
		// embedding_dim (tryRepair). When no embedder is configured or the embed
		// failed, leave embedding_dim NULL — there is no stale dim claim to clear.
		p.tryRepair(ctx, &mem, vecs[i], errs[i], stats)
	}
	return found, processed
}

// probeExistingVectors returns, per candidate, the dim of any stored facet-0
// vector that survived (the desync case). It probes through the vector-store
// abstraction (GetByIDs), not raw memory_vectors_<dim> SQL, so pgvector, HNSW,
// and Qdrant deployments are covered identically, and stops once every candidate
// has been located.
func (p *EmbeddingBackfillPhase) probeExistingVectors(ctx context.Context, cycle *model.DreamCycle, rows []model.Memory) map[uuid.UUID]int {
	existingDim := make(map[uuid.UUID]int, len(rows))
	if p.vectorStore == nil || len(rows) == 0 {
		return existingDim
	}
	ids := make([]uuid.UUID, len(rows))
	for i := range rows {
		ids[i] = rows[i].ID
	}
	for _, dim := range backfillDims() {
		if len(existingDim) == len(ids) {
			break // every candidate already located; remaining dims add nothing
		}
		got, err := p.vectorStore.GetByIDs(ctx, storage.VectorKindMemory, ids, dim)
		if err != nil {
			slog.Warn("dreaming: embedding backfill null-dim probe failed",
				"cycle", cycle.ID, "dim", dim, "err", err)
			continue
		}
		for id, vec := range got {
			if len(vec) == 0 {
				continue
			}
			if _, seen := existingDim[id]; !seen {
				existingDim[id] = dim
			}
		}
	}
	return existingDim
}

// bestEmbedDim returns the embedder's preferred dim, or 0 when no embedder is
// configured (embedForBackfill then returns errBackfillNoEmbedder and the row
// is left for a future cycle).
func (p *EmbeddingBackfillPhase) bestEmbedDim() int {
	if p.embedder == nil {
		return 0
	}
	ep := p.embedder()
	if ep == nil {
		return 0
	}
	return storage.BestEmbeddingDimension(ep.Dimensions())
}

// embedForBackfill runs only the embedding call for a divergent row. It does
// no DB writes and no stats mutation, so it is safe to call concurrently (the
// budget is mutex-safe). Returns errBackfillNoEmbedder when no embedder is
// configured (the caller clears the dim without counting an error) or
// errBackfillEmptyEmbedding when the provider returns no usable vector.
func (p *EmbeddingBackfillPhase) embedForBackfill(ctx context.Context, mem *model.Memory, dim int, budget *TokenBudget) ([]float32, error) {
	if p.embedder == nil {
		return nil, errBackfillNoEmbedder
	}
	ep := p.embedder()
	if ep == nil {
		return nil, errBackfillNoEmbedder
	}

	inputs := []string{mem.Content}
	resp, _, err := WrapLLMCall(ctx, budget, OpEmbedBackfill, ep.Name(),
		mem.ID.String(),
		func(ctx context.Context) (*provider.EmbeddingResponse, *provider.TokenUsage, error) {
			ctx = provider.WithOperation(ctx, provider.OperationEmbedding)
			ctx = provider.WithMemoryID(ctx, mem.ID)
			ctx = provider.WithNamespaceID(ctx, mem.NamespaceID)
			r, e := ep.Embed(ctx, &provider.EmbeddingRequest{
				Input:     inputs,
				Dimension: dim,
			})
			return r, usageOrEstimateEmbed(r, inputs), e
		})
	if err != nil {
		slog.Warn("dreaming: embedding backfill re-embed failed",
			"memory", mem.ID, "dim", dim, "err", err)
		return nil, err
	}
	if resp == nil || len(resp.Embeddings) == 0 || len(resp.Embeddings[0]) == 0 {
		return nil, errBackfillEmptyEmbedding
	}
	return resp.Embeddings[0], nil
}

// tryRepair writes the vector produced by embedForBackfill. embedErr carries
// the embed outcome: errBackfillNoEmbedder leaves the row for clearDim with no
// error penalty (the original embedder-unavailable path), while any other
// non-nil error or an empty vec counts toward embed_errors. Returns true on a
// successful write; false on any failure so the caller falls back to clearDim.
func (p *EmbeddingBackfillPhase) tryRepair(ctx context.Context, mem *model.Memory, vec []float32, embedErr error, stats map[string]any) bool {
	if errors.Is(embedErr, errBackfillNoEmbedder) {
		return false
	}
	if embedErr != nil || len(vec) == 0 {
		stats["embed_errors"] = stats["embed_errors"].(int) + 1
		return false
	}

	actualDim := len(vec)

	if err := p.vectorStore.Upsert(ctx, storage.VectorKindMemory, mem.ID, mem.NamespaceID, vec, actualDim); err != nil {
		slog.Warn("dreaming: embedding backfill upsert failed",
			"memory", mem.ID, "dim", actualDim, "err", err)
		stats["upsert_errors"] = stats["upsert_errors"].(int) + 1
		return false
	}

	// If the embedder picked a different dim than the row recorded
	// (model swap, dim renegotiation), sync the row's embedding_dim.
	// Otherwise the row is already consistent; no Update needed.
	if mem.EmbeddingDim == nil || *mem.EmbeddingDim != actualDim {
		d := actualDim
		mem.EmbeddingDim = &d
		if err := p.memWriter.UpdateEmbeddingDim(ctx, mem.ID, actualDim); err != nil {
			slog.Warn("dreaming: embedding backfill dim sync failed",
				"memory", mem.ID, "dim", actualDim, "err", err)
			stats["update_errors"] = stats["update_errors"].(int) + 1
			return false
		}
	}
	stats["repaired"] = stats["repaired"].(int) + 1
	return true
}

// clearDim drops embedding_dim on the row so it stops advertising a
// vector that no longer exists. The memory remains usable via tag,
// keyword, and graph recall; vector recall will pick it back up after
// the next content edit triggers a re-embed at the write path.
func (p *EmbeddingBackfillPhase) clearDim(ctx context.Context, mem *model.Memory, stats map[string]any) {
	mem.EmbeddingDim = nil
	if err := p.memWriter.ClearEmbeddingDim(ctx, mem.ID, mem.NamespaceID); err != nil {
		slog.Warn("dreaming: embedding backfill clear dim failed",
			"memory", mem.ID, "err", err)
		stats["update_errors"] = stats["update_errors"].(int) + 1
		return
	}
	stats["cleared"] = stats["cleared"].(int) + 1
}

func (p *EmbeddingBackfillPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]any, budget *TokenBudget, tokensBefore int) {
	if budget != nil {
		stats["tokens_spent"] = budget.Used() - tokensBefore
		stats["budget_remaining"] = budget.Remaining()
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseEmbeddingBackfill, "",
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
