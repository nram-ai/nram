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

// EmbeddingBackfillPhase repairs rows whose embedding_dim is set but
// whose memory_vectors_<dim> row is missing. For each divergent row it
// either re-embeds and writes a fresh vector, or clears embedding_dim
// so the row state matches the vector store. Runs before paraphrase
// dedup so the downstream phase sees the repaired state in the same
// cycle.
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
				slog.Info("dreaming: embedding backfill progress",
					"cycle", cycle.ID, "dim", dim,
					"memory", visited, "of", cap)
			}
			mem := toProcess[k]
			if p.tryRepair(ctx, &mem, vecs[k], errs[k], stats) {
				continue
			}
			p.clearDim(ctx, &mem, stats)
		}
	}

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
