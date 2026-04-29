package dreaming

import (
	"context"
	"log/slog"
	"sort"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
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
func (p *EmbeddingBackfillPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (bool, error) {
	if p.settings != nil && !p.settings.ResolveBool(ctx, service.SettingDreamEmbeddingBackfillEnabled, "global") {
		return false, nil
	}
	if p.repairer == nil || p.memWriter == nil || p.vectorStore == nil {
		return false, nil
	}

	cap, _ := p.settings.ResolveInt(ctx, service.SettingDreamEmbeddingBackfillCapPerCycle, "global")
	if cap <= 0 {
		cap = 200
	}

	stats := map[string]interface{}{
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
		for i := range toProcess {
			visited++
			mem := toProcess[i]
			if p.tryRepair(ctx, &mem, dim, stats) {
				continue
			}
			p.clearDim(ctx, &mem, stats)
		}
	}

	stats["candidates"] = foundTotal
	stats["visited"] = visited
	p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)

	return foundTotal > visited, nil
}

// tryRepair re-embeds the memory and writes a fresh vector. Returns true
// on success; false on embedder unavailability, embedder failure, or
// persistent vector-store error. On false the caller falls back to
// clearDim so the divergent row stops claiming a vector.
func (p *EmbeddingBackfillPhase) tryRepair(ctx context.Context, mem *model.Memory, dim int, stats map[string]interface{}) bool {
	if p.embedder == nil {
		return false
	}
	ep := p.embedder()
	if ep == nil {
		return false
	}

	embedCtx := provider.WithOperation(ctx, provider.OperationEmbedding)
	embedCtx = provider.WithMemoryID(embedCtx, mem.ID)
	embedCtx = provider.WithNamespaceID(embedCtx, mem.NamespaceID)

	resp, err := ep.Embed(embedCtx, &provider.EmbeddingRequest{
		Input:     []string{mem.Content},
		Dimension: dim,
	})
	if err != nil {
		slog.Warn("dreaming: embedding backfill re-embed failed",
			"memory", mem.ID, "dim", dim, "err", err)
		stats["embed_errors"] = stats["embed_errors"].(int) + 1
		return false
	}
	if resp == nil || len(resp.Embeddings) == 0 || len(resp.Embeddings[0]) == 0 {
		stats["embed_errors"] = stats["embed_errors"].(int) + 1
		return false
	}

	vec := resp.Embeddings[0]
	actualDim := len(vec)

	if err := p.vectorStore.Upsert(ctx, storage.VectorKindMemory, mem.ID, mem.NamespaceID, vec, actualDim); err != nil {
		slog.Warn("dreaming: embedding backfill upsert failed",
			"memory", mem.ID, "dim", actualDim, "err", err)
		stats["upsert_errors"] = stats["upsert_errors"].(int) + 1
		return false
	}

	// If the embedder picked a different dim than the row recorded
	// (model swap, dim renegotiation), sync the row's embedding_dim.
	// Otherwise the row is already consistent — no Update needed.
	if mem.EmbeddingDim == nil || *mem.EmbeddingDim != actualDim {
		d := actualDim
		mem.EmbeddingDim = &d
		if err := p.memWriter.Update(ctx, mem); err != nil {
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
func (p *EmbeddingBackfillPhase) clearDim(ctx context.Context, mem *model.Memory, stats map[string]interface{}) {
	mem.EmbeddingDim = nil
	if err := p.memWriter.Update(ctx, mem); err != nil {
		slog.Warn("dreaming: embedding backfill clear dim failed",
			"memory", mem.ID, "err", err)
		stats["update_errors"] = stats["update_errors"].(int) + 1
		return
	}
	stats["cleared"] = stats["cleared"].(int) + 1
}

func (p *EmbeddingBackfillPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]interface{}, budget *TokenBudget, tokensBefore int) {
	if budget != nil {
		stats["tokens_spent"] = budget.Used() - tokensBefore
		stats["budget_remaining"] = budget.Remaining()
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseEmbeddingBackfill,
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
