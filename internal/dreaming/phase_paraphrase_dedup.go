package dreaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ParaphraseCheckedStampKey marks a memory as visited by the paraphrase
// dedup sweep. Survivors carry this stamp so the sweep skips them next
// cycle. Re-checking happens when memory.UpdatedAt advances past the
// stamp (content edits, supersession, contradiction haircuts) — the
// staleness check is `stamp < UpdatedAt`.
const ParaphraseCheckedStampKey = "paraphrase_checked_at"

// ParaphraseDedupPhase sweeps every namespace memory looking for
// near-paraphrase pairs the contradiction phase's anchor walk does not
// pair. The contradiction phase only compares each anchor against its
// K=4 nearest neighbours, which leaves user-source duplicates uncaught
// when neither side is in the other's top-K window. This sweep visits
// every eligible memory and runs vector_store.Search(top-K) directly,
// so two duplicates only need to be top-K of EACH OTHER (not of every
// other anchor) to get paired.
//
// Pure vector ops — no LLM calls, so token budget is not consumed.
type ParaphraseDedupPhase struct {
	memories     MemoryReader
	memWriter    MemoryWriter
	vectorStore  storage.VectorStore
	vectorPurger VectorPurger
	embedder     func() provider.EmbeddingProvider
	settings     SettingsResolver
}

// NewParaphraseDedupPhase constructs the phase. embedder is used only as
// a fallback to discover the storage dim when no memory in the namespace
// has a recorded embedding_dim yet.
func NewParaphraseDedupPhase(
	memories MemoryReader,
	memWriter MemoryWriter,
	vectorStore storage.VectorStore,
	vectorPurger VectorPurger,
	embedder func() provider.EmbeddingProvider,
	settings SettingsResolver,
) *ParaphraseDedupPhase {
	return &ParaphraseDedupPhase{
		memories:     memories,
		memWriter:    memWriter,
		vectorStore:  vectorStore,
		vectorPurger: vectorPurger,
		embedder:     embedder,
		settings:     settings,
	}
}

func (p *ParaphraseDedupPhase) Name() string { return model.DreamPhaseParaphraseDedup }

func (p *ParaphraseDedupPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (bool, error) {
	if p.settings != nil && !p.settings.ResolveBool(ctx, service.SettingDreamParaphraseEnabled, "global") {
		return false, nil
	}
	if p.vectorStore == nil {
		return false, nil
	}

	threshold := resolveFraction(ctx, p.settings, service.SettingDreamParaphraseThreshold, 0.97)
	cap, _ := p.settings.ResolveInt(ctx, service.SettingDreamParaphraseCapPerCycle, "global")
	if cap <= 0 {
		cap = 500
	}
	topK, _ := p.settings.ResolveInt(ctx, service.SettingDreamParaphraseTopK, "global")
	if topK <= 0 {
		topK = 5
	}

	memories, err := p.memories.ListByNamespace(ctx, cycle.NamespaceID, 5000, 0)
	if err != nil {
		return false, fmt.Errorf("paraphrase dedup: list memories: %w", err)
	}

	stats := map[string]interface{}{
		"sub_phase":             model.DreamPhaseParaphraseDedup,
		"candidates":            0,
		"visited":               0,
		"superseded":            0,
		"no_vector":             0,
		"no_vector_demoted":     0,
		"no_vector_low_novelty": 0,
		"no_vector_unknown":     0,
		"filtered_superseded":   0,
		"filtered_demoted":      0,
		"filtered_unembedded":   0,
		"vector_errors":         0,
		"update_errors":         0,
		"per_cycle_cap":         cap,
		"top_k":                 topK,
		"threshold":             threshold,
	}
	tokensBefore := 0
	if budget != nil {
		tokensBefore = budget.Used()
	}

	stampMarker := []byte(ParaphraseCheckedStampKey)
	type candidate struct {
		mem  model.Memory
		meta map[string]interface{}
	}
	eligible := make([]candidate, 0, len(memories))
	for i := range memories {
		m := memories[i]
		if m.DeletedAt != nil {
			continue
		}
		if m.SupersededBy != nil {
			stats["filtered_superseded"] = stats["filtered_superseded"].(int) + 1
			continue
		}
		if m.Confidence == 0 {
			// Demoted by the audit phase — vector already purged.
			stats["filtered_demoted"] = stats["filtered_demoted"].(int) + 1
			continue
		}
		if m.EmbeddingDim == nil || *m.EmbeddingDim == 0 {
			stats["filtered_unembedded"] = stats["filtered_unembedded"].(int) + 1
			continue
		}
		// Fast path: no stamp at all → eligible without JSON decode.
		if !bytes.Contains(m.Metadata, stampMarker) {
			eligible = append(eligible, candidate{mem: m, meta: map[string]interface{}{}})
			continue
		}
		meta := decodeMetadata(m.Metadata)
		if isParaphraseStale(&m, meta) {
			eligible = append(eligible, candidate{mem: m, meta: meta})
		}
	}
	stats["candidates"] = len(eligible)

	if len(eligible) == 0 {
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return false, nil
	}

	// Resolve dim from the first memory that has one stamped. Falls back
	// to a probe of the live embedder if no memory in the namespace has
	// embedding_dim recorded yet.
	dim := 0
	for i := range memories {
		if memories[i].EmbeddingDim != nil && *memories[i].EmbeddingDim > 0 {
			dim = *memories[i].EmbeddingDim
			break
		}
	}
	if dim == 0 && p.embedder != nil {
		emb := p.embedder()
		if emb != nil {
			probeResp, perr := emb.Embed(ctx, &provider.EmbeddingRequest{Input: []string{"probe"}})
			if perr == nil && probeResp != nil && len(probeResp.Embeddings) > 0 {
				dim = len(probeResp.Embeddings[0])
			}
		}
	}
	if dim == 0 {
		slog.Warn("dreaming: paraphrase dedup skipped; could not resolve embedder dim",
			"cycle", cycle.ID, "namespace", cycle.NamespaceID)
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return false, nil
	}

	supersededInCycle := map[uuid.UUID]bool{}
	visited := 0

	// Batch-fetch every eligible anchor's vector in one round-trip rather
	// than one GetByIDs per iteration.
	eligibleIDs := make([]uuid.UUID, len(eligible))
	for i := range eligible {
		eligibleIDs[i] = eligible[i].mem.ID
	}
	vectorsByID, err := p.vectorStore.GetByIDs(ctx, storage.VectorKindMemory, eligibleIDs, dim)
	if err != nil {
		stats["vector_errors"] = 1
		p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)
		return false, fmt.Errorf("paraphrase dedup: batch fetch vectors: %w", err)
	}

	// Index the namespace snapshot so the inner result loop resolves
	// neighbours by map lookup instead of a per-result GetByID. In-cycle
	// supersessions mutate these pointers via applySupersede, so the
	// SupersededBy guard below stays accurate without a fresh DB read.
	memoryByID := make(map[uuid.UUID]*model.Memory, len(memories))
	for i := range memories {
		memoryByID[memories[i].ID] = &memories[i]
	}

	for i := range eligible {
		if visited >= cap {
			break
		}
		anchor := &eligible[i].mem
		if supersededInCycle[anchor.ID] {
			// Already lost a paraphrase round earlier this cycle — skip;
			// the survivor will carry the stamp.
			continue
		}
		visited++

		anchorVec, ok := vectorsByID[anchor.ID]
		if !ok || len(anchorVec) == 0 {
			// Eligibility filter already excludes Confidence==0 and
			// EmbeddingDim==nil; non-zero no_vector_unknown points at
			// drift the embedding-backfill phase will repair next cycle.
			stats["no_vector"] = stats["no_vector"].(int) + 1
			switch {
			case anchor.Confidence == 0:
				stats["no_vector_demoted"] = stats["no_vector_demoted"].(int) + 1
			case isMetaLowNovelty(eligible[i].meta):
				stats["no_vector_low_novelty"] = stats["no_vector_low_novelty"].(int) + 1
			default:
				stats["no_vector_unknown"] = stats["no_vector_unknown"].(int) + 1
			}
			_ = p.stampParaphrase(ctx, anchor, eligible[i].meta)
			continue
		}

		results, err := p.vectorStore.Search(ctx, storage.VectorKindMemory, anchorVec, anchor.NamespaceID, dim, topK+1)
		if err != nil {
			stats["vector_errors"] = stats["vector_errors"].(int) + 1
			continue
		}

		anchorBecameLoser := false
		for _, r := range results {
			if r.ID == anchor.ID {
				continue // skip self
			}
			if r.Score < threshold {
				break // results are sorted desc by score
			}
			if supersededInCycle[r.ID] {
				continue
			}

			other, ok := memoryByID[r.ID]
			if !ok {
				continue
			}
			if other.DeletedAt != nil || other.SupersededBy != nil {
				continue
			}

			winner, loser := pickParaphraseWinner(anchor, other)
			if err := p.applySupersede(ctx, cycle, logger, loser, winner, r.Score); err != nil {
				stats["update_errors"] = stats["update_errors"].(int) + 1
				continue
			}
			supersededInCycle[loser.ID] = true
			stats["superseded"] = stats["superseded"].(int) + 1

			if loser.ID == anchor.ID {
				anchorBecameLoser = true
				break
			}
			// Loser was the neighbour. Anchor is still live and may have
			// more matches further down the result list — continue.
		}

		if !anchorBecameLoser {
			_ = p.stampParaphrase(ctx, anchor, eligible[i].meta)
		}
	}
	stats["visited"] = visited

	p.writePhaseSummary(ctx, logger, stats, budget, tokensBefore)

	residual := visited < len(eligible)
	return residual, nil
}

// pickParaphraseWinner returns (winner, loser) for two paraphrases.
// Higher confidence wins; ties resolve to the older CreatedAt so the
// stable historic memory survives over a recent rewrite.
func pickParaphraseWinner(a, b *model.Memory) (*model.Memory, *model.Memory) {
	if a.Confidence > b.Confidence {
		return a, b
	}
	if b.Confidence > a.Confidence {
		return b, a
	}
	if a.CreatedAt.Before(b.CreatedAt) {
		return a, b
	}
	return b, a
}

func (p *ParaphraseDedupPhase) applySupersede(
	ctx context.Context,
	cycle *model.DreamCycle,
	logger *DreamLogWriter,
	loser, winner *model.Memory,
	cosine float64,
) error {
	now := time.Now().UTC()
	loser.SupersededBy = &winner.ID
	loser.SupersededAt = &now
	loser.UpdatedAt = now
	loser.EmbeddingDim = nil // vector is purged below; keep row state in sync
	if err := p.memWriter.Update(ctx, loser); err != nil {
		slog.Warn("dreaming: paraphrase dedup supersede update failed",
			"memory", loser.ID, "winner", winner.ID, "err", err)
		return err
	}
	if p.vectorPurger != nil {
		if err := p.vectorPurger.Delete(ctx, storage.VectorKindMemory, loser.ID); err != nil {
			slog.Warn("dreaming: paraphrase dedup vector purge failed",
				"memory", loser.ID, "err", err)
		}
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseParaphraseDedup,
		model.DreamOpParaphraseSuperseded, "memory", loser.ID,
		nil, map[string]interface{}{
			"superseded_by": winner.ID.String(),
			"cosine":        cosine,
			"reason":        "paraphrase_dedup_sweep",
		})
	return nil
}

// stampParaphrase records the visit stamp anchored to mem.UpdatedAt
// via UpdateMetadata so the staleness check (stamp < UpdatedAt) does
// not self-invalidate next cycle.
func (p *ParaphraseDedupPhase) stampParaphrase(ctx context.Context, mem *model.Memory, meta map[string]interface{}) error {
	if meta == nil {
		meta = map[string]interface{}{}
	}
	meta[ParaphraseCheckedStampKey] = mem.UpdatedAt.UTC().Format(time.RFC3339Nano)
	encoded, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal paraphrase stamp: %w", err)
	}
	mem.Metadata = encoded
	if err := p.memWriter.UpdateMetadata(ctx, mem.ID, mem.NamespaceID, encoded); err != nil {
		return fmt.Errorf("persist paraphrase stamp: %w", err)
	}
	return nil
}

// isMetaLowNovelty reports whether the memory metadata carries the
// low_novelty marker set by the consolidation novelty audit on demote.
// Used by the no_vector categorical breakdown to classify a missing
// vector as "expected absence" vs. "unexplained drift."
func isMetaLowNovelty(meta map[string]interface{}) bool {
	if meta == nil {
		return false
	}
	v, ok := meta["low_novelty"].(bool)
	return ok && v
}

// isParaphraseStale mirrors isStale (contradiction phase): no stamp,
// malformed stamp, or stamp strictly before UpdatedAt → eligible.
func isParaphraseStale(mem *model.Memory, meta map[string]interface{}) bool {
	raw, ok := meta[ParaphraseCheckedStampKey]
	if !ok {
		return true
	}
	s, ok := raw.(string)
	if !ok || s == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return true
		}
	}
	return t.Before(mem.UpdatedAt)
}

func (p *ParaphraseDedupPhase) writePhaseSummary(ctx context.Context, logger *DreamLogWriter, stats map[string]interface{}, budget *TokenBudget, tokensBefore int) {
	if budget != nil {
		stats["tokens_spent"] = budget.Used() - tokensBefore
		stats["budget_remaining"] = budget.Remaining()
	}
	_ = logger.LogOperation(ctx, model.DreamPhaseParaphraseDedup,
		model.DreamOpPhaseSummary, "phase", uuid.Nil, nil, stats)
}
