package dreaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// ContradictionsCheckedStampKey is the metadata key stamped on memories once
// they have been compared against their top-K semantic neighbours in a cycle.
// A memory is considered fresh for contradiction purposes when the stamp
// exists and is not strictly before its UpdatedAt; any other state marks the
// memory as stale and eligible for re-checking.
const ContradictionsCheckedStampKey = "contradictions_checked_at"

// Winner values emitted by the contradiction LLM judge in the response's
// "winner" field. WinnerTie also covers the legacy-prompt empty-field case
// after normalization.
const (
	WinnerSideA = "a"
	WinnerSideB = "b"
	WinnerTie   = "tie"
)

// defaultContradictionCap bounds the number of pair-check LLM calls the
// phase may issue per cycle. Exposed as SettingDreamContradictionCap so
// operators can raise it during a first-pass drain and restore it after.
const defaultContradictionCap = 30

// defaultContradictionNeighbors bounds the per-memory work. A memory is
// compared against at most K semantic neighbours per visit. K is deliberately
// small so "fully dispatched" is reachable under defaultContradictionCap for
// namespaces of realistic size (K*anchors_per_cycle ≤ cap).
const defaultContradictionNeighbors = 4

// ContradictionPhase detects contradictions between semantically-close pairs
// of memories. Per-memory work is bounded via an embedding top-K prefilter;
// progress across cycles is tracked via the ContradictionsCheckedStampKey
// metadata marker so the phase can report residual=false once every memory
// has been visited since its last update.
type ContradictionPhase struct {
	memories         MemoryReader
	memWriter        MemoryWriter
	lineage          LineageWriter
	llmProvider      LLMProviderFunc
	embedderProvider EmbeddingProviderFunc
	settings         SettingsResolver
	vectorStore      storage.VectorStore
	vectorPurger     VectorPurger
}

// AttachVectorStore wires a VectorStore so the phase reads stored embeddings
// instead of re-embedding the namespace every cycle. Nil disables the read
// path and falls back to the legacy embed-everything branch.
func (p *ContradictionPhase) AttachVectorStore(vs storage.VectorStore) {
	p.vectorStore = vs
}

// AttachVectorPurger wires a VectorPurger so paraphrase auto-supersede also
// drops the loser's vector. Without it, a superseded loser remains in the
// active index and can resurface as a neighbour for later anchors in the
// same cycle.
func (p *ContradictionPhase) AttachVectorPurger(vp VectorPurger) {
	p.vectorPurger = vp
}

// NewContradictionPhase creates a new contradiction detection phase.
// When embedderProvider returns nil (either absent or transiently failed),
// the phase degrades to a deterministic ID-ordered neighbour walk.
// token_usage rows are written by the UsageRecordingProvider middleware
// wrapping the registry-issued providers; no per-phase recorder is needed.
func NewContradictionPhase(
	memories MemoryReader,
	memWriter MemoryWriter,
	lineage LineageWriter,
	llmProvider LLMProviderFunc,
	embedderProvider EmbeddingProviderFunc,
	settings SettingsResolver,
) *ContradictionPhase {
	return &ContradictionPhase{
		memories:         memories,
		memWriter:        memWriter,
		lineage:          lineage,
		llmProvider:      llmProvider,
		embedderProvider: embedderProvider,
		settings:         settings,
	}
}

func (p *ContradictionPhase) Name() string { return model.DreamPhaseContradictions }

// staleMemory carries a memory alongside its pre-decoded metadata so the
// stamp-write path does not have to re-parse on completion.
type staleMemory struct {
	Mem  model.Memory
	Meta map[string]interface{}
}

// mirrorToStale propagates the latest writable fields from mem back into the
// stale-index entry so the post-loop stamp Update — which serializes every
// column from stale[i].Mem — does not clobber haircut confidence or
// supersession markers written earlier in the cycle.
func mirrorToStale(staleByID map[uuid.UUID]*model.Memory, mem *model.Memory) {
	s, ok := staleByID[mem.ID]
	if !ok {
		return
	}
	s.Confidence = mem.Confidence
	s.UpdatedAt = mem.UpdatedAt
	s.SupersededBy = mem.SupersededBy
	s.SupersededAt = mem.SupersededAt
}

// pairKey canonicalises a pair of memory IDs so (A,B) and (B,A) dedupe.
type pairKey struct {
	low  uuid.UUID
	high uuid.UUID
}

func orderedPairKey(a, b uuid.UUID) pairKey {
	if bytes.Compare(a[:], b[:]) <= 0 {
		return pairKey{low: a, high: b}
	}
	return pairKey{low: b, high: a}
}

func (p *ContradictionPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (bool, error) {
	// Stamp namespace context once so every provider call emitted by this
	// phase lands a token_usage row attributed to the right scope. The
	// UsageRecordingProvider middleware reads namespace_id from ctx and,
	// when no UsageContext is pre-stamped, falls back to its injected
	// resolver to populate org/user/project.
	ctx = provider.WithNamespaceID(ctx, cycle.NamespaceID)
	llm := p.llmProvider()
	if llm == nil {
		slog.Info("dreaming: no LLM provider for contradiction detection, skipping")
		return false, nil
	}

	memories, err := p.memories.ListByNamespace(ctx, cycle.NamespaceID, 500, 0)
	if err != nil {
		return false, err
	}

	if len(memories) < 2 {
		return false, nil
	}

	cap, _ := p.settings.ResolveInt(ctx, service.SettingDreamContradictionCap, "global")
	if cap <= 0 {
		cap = defaultContradictionCap
	}

	stale := p.collectStale(memories)
	if len(stale) == 0 {
		return false, nil
	}

	slog.Info("dreaming: contradiction phase starting",
		"cycle", cycle.ID, "memories", len(memories), "stale", len(stale),
		"cap", cap, "neighbors_per_anchor", defaultContradictionNeighbors)

	pairs, fullyDispatched, embedTokens, vectorsByID, selErr := p.selectNeighborPairs(ctx, stale, memories, cap)
	if selErr != nil {
		slog.Warn("dreaming: neighbour selection failed; degrading to ID-ordered walk",
			"cycle", cycle.ID, "err", selErr)
	}
	// token_usage rows for the embedding probe + batch are written by
	// the UsageRecordingProvider middleware. Here we only spend against
	// the dream-cycle budget.
	if embedTokens > 0 {
		_ = budget.Spend(embedTokens)
	}

	promptTemplate, _ := p.settings.Resolve(ctx, service.SettingDreamContradictionPrompt, "global")

	// Index stale by ID so haircut/supersede updates can be mirrored back.
	// The post-loop stamp pass writes Update on each stale[i].Mem with all
	// columns; without this mirror the stamp would overwrite the haircut
	// confidence with the pre-haircut value.
	staleByID := make(map[uuid.UUID]*model.Memory, len(stale))
	for i := range stale {
		staleByID[stale[i].Mem.ID] = &stale[i].Mem
	}

	// Haircut settings are stable for the duration of the cycle; resolve once
	// up front rather than re-resolving on every contradiction.
	loserBase := resolveFraction(ctx, p.settings, service.SettingDreamContradictionLoserHaircut, 0.85)
	winnerBase := resolveFraction(ctx, p.settings, service.SettingDreamContradictionWinnerHaircut, 0.97)
	tieBase := resolveFraction(ctx, p.settings, service.SettingDreamContradictionTieHaircut, 0.92)
	supersedeThreshold := resolveFraction(ctx, p.settings, service.SettingDreamSupersessionThreshold, 0.85)
	paraphraseEnabled := p.settings.ResolveBool(ctx, service.SettingDreamContradictionParaphraseEnabled, "global")
	paraphraseThreshold := resolveFraction(ctx, p.settings, service.SettingDreamContradictionParaphraseThreshold, 0.97)

	contradictions := 0
	paraphrasesSuperseded := 0
	budgetStopped := false
	for idx, pair := range pairs {
		if budget.Exhausted() {
			budgetStopped = true
			break
		}

		// Skip the LLM judge for near-duplicate paraphrases — the strict
		// contradiction prompt is intentionally blind to them.
		if paraphraseEnabled && vectorsByID != nil {
			va, okA := vectorsByID[pair[0].ID]
			vb, okB := vectorsByID[pair[1].ID]
			if okA && okB && len(va) == len(vb) && len(va) > 0 {
				sim := hnsw.CosineSimilarity(va, vb)
				if float64(sim) >= paraphraseThreshold {
					p.paraphraseSupersede(ctx, cycle, logger, &pair[0], &pair[1], float64(sim), staleByID)
					paraphrasesSuperseded++
					continue
				}
			}
		}

		// Pre-flight budget check using the 4-bytes-per-token heuristic on
		// the prompt plus the per-call output cap. Prevents starting calls
		// we can't afford to record.
		estPrompt := fmt.Sprintf(promptTemplate, pair[0].Content, pair[1].Content)
		estCost := EstimateTokens(estPrompt) + budget.PerCallCap()
		if budget.Remaining() < estCost {
			slog.Info("dreaming: contradiction call skipped (estimated cost exceeds remaining budget)",
				"estimate", estCost, "remaining", budget.Remaining())
			budgetStopped = true
			break
		}

		pairStart := time.Now()
		found, winner, explanation, usage, err := p.checkContradiction(ctx, llm, &pair[0], &pair[1], estPrompt, budget)
		pairDur := time.Since(pairStart)

		// Account for usage before handling the error. Parse-error paths
		// still return non-nil usage from the LLM call. token_usage rows
		// are written by the UsageRecordingProvider middleware; here we
		// only charge the dream-cycle budget.
		var spendErr error
		callTokens := 0
		if usage != nil {
			callTokens = usage.TotalTokens
			spendErr = budget.Spend(usage.TotalTokens)
		}

		if err != nil {
			slog.Warn("dreaming: contradiction check failed",
				"cycle", cycle.ID, "pair", idx+1, "of", len(pairs),
				"a", pair[0].ID, "b", pair[1].ID,
				"duration_ms", pairDur.Milliseconds(), "tokens", callTokens, "err", err)
			if spendErr != nil {
				budgetStopped = true
				break
			}
			continue
		}

		slog.Info("dreaming: contradiction check",
			"cycle", cycle.ID, "pair", idx+1, "of", len(pairs),
			"a", pair[0].ID, "b", pair[1].ID,
			"found", found, "duration_ms", pairDur.Milliseconds(),
			"tokens", callTokens, "budget_remaining", budget.Remaining())

		if spendErr != nil {
			budgetStopped = true
			break
		}

		if !found {
			continue
		}

		priorCount, lcErr := p.lineage.CountConflictsBetween(ctx, cycle.NamespaceID, pair[0].ID, pair[1].ID)
		if lcErr != nil {
			slog.Warn("dreaming: lineage count failed",
				"a", pair[0].ID, "b", pair[1].ID, "err", lcErr)
			continue
		}
		detection := priorCount + 1

		// Diminishing factor: 1 - (1 - base) / N. n=1 applies the full base
		// haircut; subsequent detections of the same pair shrink the gap
		// to 1.0 so cumulative drift is bounded.
		diminish := func(base float64) float64 {
			return 1.0 - (1.0-base)/float64(detection)
		}

		// Empty winner means the operator's prompt predates the field; treat
		// as tie so legacy deployments degrade gracefully to symmetric haircuts.
		normalized := winner
		if normalized != WinnerSideA && normalized != WinnerSideB {
			normalized = WinnerTie
		}

		now := time.Now().UTC()
		var loserMem, winnerMem *model.Memory
		var loserFactor, winnerFactor float64
		switch normalized {
		case WinnerSideA:
			winnerMem, loserMem = &pair[0], &pair[1]
			winnerFactor, loserFactor = diminish(winnerBase), diminish(loserBase)
		case WinnerSideB:
			winnerMem, loserMem = &pair[1], &pair[0]
			winnerFactor, loserFactor = diminish(winnerBase), diminish(loserBase)
		default:
			loserFactor = diminish(tieBase)
			winnerFactor = diminish(tieBase)
		}

		// Conditional supersede: when the winner was already well-established
		// before the haircut, mark the loser as superseded so the supersede
		// prune branch (with its 7d clock from SupersededAt) cleans it up
		// faster than waiting for confidence drift to reach zero. Set the
		// fields here so the haircut Update below carries them in one write.
		loserSuperseded := false
		if winnerMem != nil && winnerMem.Confidence > supersedeThreshold {
			loserMem.SupersededBy = &winnerMem.ID
			loserMem.SupersededAt = &now
			loserMem.EmbeddingDim = nil
			loserSuperseded = true
		}

		applyHaircut := func(mem *model.Memory, factor float64) {
			mem.Confidence = math.Max(0.0, mem.Confidence*factor)
			mem.UpdatedAt = now
			if err := p.memWriter.Update(ctx, mem); err != nil {
				slog.Warn("dreaming: contradiction haircut update failed",
					"memory", mem.ID, "err", err)
			}
			mirrorToStale(staleByID, mem)
		}

		if normalized == WinnerTie {
			applyHaircut(&pair[0], loserFactor)
			applyHaircut(&pair[1], winnerFactor)
		} else {
			applyHaircut(loserMem, loserFactor)
			applyHaircut(winnerMem, winnerFactor)
		}

		if loserSuperseded && p.vectorPurger != nil {
			if err := p.vectorPurger.Delete(ctx, storage.VectorKindMemory, loserMem.ID); err != nil {
				slog.Warn("dreaming: contradiction haircut vector purge failed",
					"memory", loserMem.ID, "err", err)
			}
		}

		edgeCtx, _ := json.Marshal(map[string]interface{}{
			"detection_count": detection,
			"winner":          normalized,
		})
		lineageEntry := &model.MemoryLineage{
			ID:          uuid.New(),
			NamespaceID: cycle.NamespaceID,
			MemoryID:    pair[0].ID,
			ParentID:    &pair[1].ID,
			Relation:    model.LineageConflictsWith,
			Context:     edgeCtx,
		}
		if err := p.lineage.Create(ctx, lineageEntry); err != nil {
			slog.Warn("dreaming: lineage creation failed", "err", err)
			continue
		}

		_ = logger.LogOperation(ctx, model.DreamPhaseContradictions,
			model.DreamOpContradictionDetected, "memory", pair[0].ID,
			nil, map[string]interface{}{
				"conflicting_id":  pair[1].ID.String(),
				"winner":          normalized,
				"detection_count": detection,
				"loser_factor":    loserFactor,
				"winner_factor":   winnerFactor,
				"explanation":     explanation,
			})

		contradictions++
	}

	// Stamp every stale anchor whose full K-slice was emitted (or deduped
	// against already-emitted pairs) before the cap tripped. Anchors that
	// were partially dispatched remain stale so the next cycle picks them
	// up. Stamping even when the budget stopped mid-loop is still safe:
	// the pairs we DID run covered this anchor's neighbour window.
	stamped := 0
	for i := range stale {
		if !fullyDispatched[stale[i].Mem.ID] {
			continue
		}
		if err := p.stampContradictionsChecked(ctx, &stale[i].Mem, stale[i].Meta); err != nil {
			slog.Warn("dreaming: contradiction stamp failed",
				"memory", stale[i].Mem.ID, "err", err)
			continue
		}
		stamped++
	}

	if contradictions > 0 || stamped > 0 || paraphrasesSuperseded > 0 {
		slog.Info("dreaming: contradiction phase summary",
			"cycle", cycle.ID, "contradictions", contradictions,
			"paraphrases_superseded", paraphrasesSuperseded,
			"stale", len(stale), "stamped", stamped,
			"budget_stopped", budgetStopped)
	}

	residual := budgetStopped || len(stale) > len(fullyDispatched)
	return residual, nil
}

// collectStale returns the subset of memories whose contradictions_checked_at
// stamp is missing or strictly before their UpdatedAt. The byte-level marker
// check up-front skips the JSON decode for any memory that cannot possibly be
// fresh (i.e. has no stamp at all).
func (p *ContradictionPhase) collectStale(memories []model.Memory) []staleMemory {
	stampMarker := []byte(ContradictionsCheckedStampKey)
	stale := make([]staleMemory, 0, len(memories))
	for i := range memories {
		m := memories[i]
		if m.DeletedAt != nil {
			continue
		}
		if !bytes.Contains(m.Metadata, stampMarker) {
			stale = append(stale, staleMemory{Mem: m, Meta: map[string]interface{}{}})
			continue
		}
		meta := decodeMetadata(m.Metadata)
		if isStale(&m, meta) {
			stale = append(stale, staleMemory{Mem: m, Meta: meta})
		}
	}
	return stale
}

// isStale reports whether a memory needs a contradiction pass. A memory is
// stale when it has no stamp at all, the stamp is malformed, or the stamp
// time is strictly before the memory's UpdatedAt. Equal stamp and UpdatedAt
// are considered fresh — the stamping path sets both to the same instant
// and we don't want a just-stamped memory to look stale on the next cycle.
func isStale(mem *model.Memory, meta map[string]interface{}) bool {
	raw, ok := meta[ContradictionsCheckedStampKey]
	if !ok {
		return true
	}
	s, ok := raw.(string)
	if !ok || s == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		// Accept the older RFC3339 form as a fallback so data written by
		// an earlier version does not get continuously re-stamped.
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return true
		}
	}
	return t.Before(mem.UpdatedAt)
}

// selectNeighborPairs picks pairs involving stale anchors. Stored vectors
// are read from the attached VectorStore first; misses are embedded once at
// the current dim and self-healed back into the store via UpsertBatch. With
// stored vectors available, each anchor's K candidates come from a
// namespace-scoped VectorStore.Search; on Search failure or unavailable
// vector the path falls back to in-process top-K over the loaded vectors,
// and finally to ID-ordered walk. Returns: the pairs in dispatch order, the
// set of anchors whose full K-slice was processed before the cap, the
// embedding-token cost to charge to the cycle budget, and the per-ID vector
// map so the paraphrase fast-path can compute cosine without re-embedding.
func (p *ContradictionPhase) selectNeighborPairs(
	ctx context.Context,
	stale []staleMemory,
	allMemories []model.Memory,
	cap int,
) ([][2]model.Memory, map[uuid.UUID]bool, int, map[uuid.UUID][]float32, error) {
	idxByID := make(map[uuid.UUID]int, len(allMemories))
	for i := range allMemories {
		idxByID[allMemories[i].ID] = i
	}

	var embedder provider.EmbeddingProvider
	if p.embedderProvider != nil {
		embedder = p.embedderProvider()
	}

	embedTokens := 0
	var selErr error

	// Probe the embedder to learn the dim it actually returns. Provider
	// Dimensions() advertises what the model can be configured for, but
	// OpenAI-compatible endpoints (Ollama, vLLM, local proxies) routinely
	// ignore the `dimensions` request parameter and emit their native size.
	// Using the requested dim as the storage key sends short vectors to a
	// wider table and pgvector rejects the upsert; using the probed dim
	// matches what the service write path stores via len(vec).
	dim := 0
	if embedder != nil {
		probeResp, probeErr := embedder.Embed(provider.WithOperation(ctx, provider.OperationDreamContradictionEmbed), &provider.EmbeddingRequest{
			Input: []string{"probe"},
		})
		if probeErr != nil || probeResp == nil || len(probeResp.Embeddings) == 0 || len(probeResp.Embeddings[0]) == 0 {
			slog.Warn("dreaming: embedder dim probe failed; skipping vector-store optimization",
				"provider", embedder.Name(), "err", probeErr)
		} else {
			dim = len(probeResp.Embeddings[0])
			if probeResp.Usage.TotalTokens > 0 {
				embedTokens += probeResp.Usage.TotalTokens
			} else {
				embedTokens += EstimateTokens("probe")
			}
		}
	}

	// Phase 1: read whatever vectors are already in the store. A missing or
	// erroring vector store leaves stored=nil and the path falls through to
	// the legacy embed-everything branch below.
	var stored map[uuid.UUID][]float32
	if p.vectorStore != nil && dim > 0 {
		ids := make([]uuid.UUID, len(allMemories))
		for i := range allMemories {
			ids[i] = allMemories[i].ID
		}
		fetched, err := p.vectorStore.GetByIDs(ctx, storage.VectorKindMemory, ids, dim)
		if err != nil {
			slog.Warn("dreaming: vector-store fetch failed; falling back to full re-embed",
				"err", err)
		} else {
			stored = fetched
		}
	}

	// Phase 2: identify misses (memories with no stored vector at this dim)
	// and embed them in one batch. On embedder-only-path (no store), all
	// memories are misses — preserves the legacy behavior.
	missIdx := make([]int, 0)
	for i := range allMemories {
		if _, ok := stored[allMemories[i].ID]; !ok {
			missIdx = append(missIdx, i)
		}
	}

	if len(missIdx) > 0 && embedder != nil {
		inputs := make([]string, len(missIdx))
		for j, i := range missIdx {
			inputs[j] = allMemories[i].Content
		}
		slog.Info("dreaming: embedding miss set for neighbour selection",
			"provider", embedder.Name(), "misses", len(inputs), "total", len(allMemories))
		embedStart := time.Now()
		resp, err := embedder.Embed(provider.WithOperation(ctx, provider.OperationDreamContradictionEmbed), &provider.EmbeddingRequest{
			Input:     inputs,
			Dimension: dim,
		})
		embedDur := time.Since(embedStart)
		if err != nil || resp == nil || len(resp.Embeddings) != len(inputs) {
			selErr = err
			slog.Warn("dreaming: neighbour embedding failed",
				"provider", embedder.Name(), "duration_ms", embedDur.Milliseconds(), "err", err)
		} else {
			if stored == nil {
				stored = make(map[uuid.UUID][]float32, len(missIdx))
			}
			for j, vec := range resp.Embeddings {
				stored[allMemories[missIdx[j]].ID] = vec
			}
			missTokens := resp.Usage.TotalTokens
			if missTokens == 0 {
				for _, s := range inputs {
					missTokens += EstimateTokens(s)
				}
			}
			embedTokens += missTokens
			slog.Info("dreaming: neighbour embedding complete",
				"provider", embedder.Name(), "count", len(inputs),
				"duration_ms", embedDur.Milliseconds(), "tokens", missTokens)

			// Self-heal: write the freshly-embedded vectors back so the next
			// cycle finds them already cached. Best-effort; log + continue
			// on error so a transient write failure doesn't block this cycle.
			if p.vectorStore != nil {
				items := make([]storage.VectorUpsertItem, len(missIdx))
				for j, i := range missIdx {
					items[j] = storage.VectorUpsertItem{
						Kind:        storage.VectorKindMemory,
						ID:          allMemories[i].ID,
						NamespaceID: allMemories[i].NamespaceID,
						Embedding:   resp.Embeddings[j],
						Dimension:   len(resp.Embeddings[j]),
					}
				}
				if uerr := p.vectorStore.UpsertBatch(ctx, items); uerr != nil {
					slog.Warn("dreaming: self-heal vector upsert failed; will retry next cycle",
						"err", uerr, "count", len(items))
				}
			}
		}
	}

	pairs := make([][2]model.Memory, 0, cap)
	seen := make(map[pairKey]struct{}, cap)
	fullyDispatched := make(map[uuid.UUID]bool, len(stale))

	for i := range stale {
		anchor := stale[i].Mem
		anchorIdx, ok := idxByID[anchor.ID]
		if !ok {
			continue
		}

		candidates := p.candidatesFor(ctx, anchor, anchorIdx, allMemories, idxByID, stored, dim)

		var capHit bool
		pairs, capHit = dispatchCandidates(anchor, candidates, allMemories, pairs, seen, cap)
		if !capHit {
			// Anchor's full K-slice was processed (emitted or deduped).
			// Later anchors whose candidates all dedupe against earlier
			// work are still fully dispatched; do not break the loop.
			fullyDispatched[anchor.ID] = true
		}
	}

	return pairs, fullyDispatched, embedTokens, stored, selErr
}

// candidatesFor returns the K nearest-neighbour candidate indices for an
// anchor, in priority order:
//  1. VectorStore.Search if both the store and the anchor's vector are
//     available — this is the cheap path that scales with the index.
//  2. In-process top-K over the loaded `stored` map — used when Search is
//     unavailable or returns an error so we still benefit from the work
//     spent loading vectors.
//  3. ID-ordered walk — final degradation when no vector data exists at all.
func (p *ContradictionPhase) candidatesFor(
	ctx context.Context,
	anchor model.Memory,
	anchorIdx int,
	allMemories []model.Memory,
	idxByID map[uuid.UUID]int,
	stored map[uuid.UUID][]float32,
	dim int,
) []int {
	anchorVec, hasVec := stored[anchor.ID]

	if p.vectorStore != nil && hasVec && dim > 0 {
		// topK+1 because Search will return the anchor itself (cosine 1.0)
		// at rank 0; we filter it out below.
		results, err := p.vectorStore.Search(ctx, storage.VectorKindMemory, anchorVec, anchor.NamespaceID, dim, defaultContradictionNeighbors+1)
		if err == nil {
			out := make([]int, 0, defaultContradictionNeighbors)
			for _, r := range results {
				if r.ID == anchor.ID {
					continue
				}
				if i, ok := idxByID[r.ID]; ok {
					out = append(out, i)
				}
				if len(out) >= defaultContradictionNeighbors {
					break
				}
			}
			if len(out) > 0 {
				return out
			}
			// Search returned nothing usable (e.g. empty index right after
			// a snapshot rebuild) — fall through to the next tier.
		} else {
			slog.Warn("dreaming: vector-store Search failed; degrading to in-process top-K",
				"anchor", anchor.ID, "err", err)
		}
	}

	if hasVec && len(stored) > 1 {
		return topKNeighborsFromMap(anchor.ID, allMemories, stored, defaultContradictionNeighbors)
	}

	return idOrderedNeighbors(anchorIdx, len(allMemories), defaultContradictionNeighbors)
}

// topKNeighborsFromMap returns the indices of the K most cosine-similar
// memories to the anchor, restricted to those whose vectors are present in
// the loaded map. Used as the Search-failed fallback so we don't have to
// re-embed if VectorStore.Search transiently errors.
func topKNeighborsFromMap(anchorID uuid.UUID, allMemories []model.Memory, stored map[uuid.UUID][]float32, k int) []int {
	anchorVec, ok := stored[anchorID]
	if !ok {
		return nil
	}
	anchorNorm := hnsw.Norm(anchorVec)
	type scored struct {
		idx int
		sim float64
	}
	scores := make([]scored, 0, len(stored))
	for i := range allMemories {
		if allMemories[i].ID == anchorID {
			continue
		}
		v, ok := stored[allMemories[i].ID]
		if !ok {
			continue
		}
		scores = append(scores, scored{idx: i, sim: hnsw.CosineSimilarityWithNorms(anchorVec, v, anchorNorm, hnsw.Norm(v))})
	}
	sort.Slice(scores, func(a, b int) bool { return scores[a].sim > scores[b].sim })
	if len(scores) > k {
		scores = scores[:k]
	}
	out := make([]int, len(scores))
	for i, s := range scores {
		out[i] = s.idx
	}
	return out
}

// idOrderedNeighbors returns the K indices after anchorIdx in the memory
// list, wrapping around. Used when no embedder is available.
func idOrderedNeighbors(anchorIdx, total, k int) []int {
	if total <= 1 {
		return nil
	}
	if k > total-1 {
		k = total - 1
	}
	out := make([]int, 0, k)
	for offset := 1; len(out) < k && offset < total; offset++ {
		j := (anchorIdx + offset) % total
		if j == anchorIdx {
			continue
		}
		out = append(out, j)
	}
	return out
}

// dispatchCandidates emits (anchor, partner) pairs for each candidate index
// that hasn't already been seen, respecting cap. Returns the updated pairs
// slice and whether the cap was hit mid-slice.
func dispatchCandidates(
	anchor model.Memory,
	candidates []int,
	allMemories []model.Memory,
	pairs [][2]model.Memory,
	seen map[pairKey]struct{},
	cap int,
) ([][2]model.Memory, bool) {
	for _, idx := range candidates {
		partner := allMemories[idx]
		key := orderedPairKey(anchor.ID, partner.ID)
		if _, dup := seen[key]; dup {
			continue
		}
		if len(pairs) >= cap {
			return pairs, true
		}
		seen[key] = struct{}{}
		pairs = append(pairs, [2]model.Memory{anchor, partner})
	}
	return pairs, false
}

// paraphraseSupersede picks the lower-confidence side of a near-duplicate
// pair and supersedes it without invoking the LLM judge. Tiebreak when the
// confidences are equal: the older CreatedAt survives — older memories are
// more likely to have downstream lineage edges, so preserving them avoids
// orphaning history.
//
// The loser is mirrored back into staleByID so the post-loop stamp Update
// preserves SupersededBy / SupersededAt. The vector purger drops the loser
// from the active store so it cannot resurface as a top-K neighbour for
// later anchors in the same cycle.
func (p *ContradictionPhase) paraphraseSupersede(
	ctx context.Context,
	cycle *model.DreamCycle,
	logger *DreamLogWriter,
	a, b *model.Memory,
	cosine float64,
	staleByID map[uuid.UUID]*model.Memory,
) {
	winner, loser := a, b
	winnerSide := "a"
	if b.Confidence > a.Confidence {
		winner, loser = b, a
		winnerSide = "b"
	} else if a.Confidence == b.Confidence {
		// Equal confidence: prefer the older CreatedAt as the survivor.
		if b.CreatedAt.Before(a.CreatedAt) {
			winner, loser = b, a
			winnerSide = "b"
		}
	}

	now := time.Now().UTC()
	loser.SupersededBy = &winner.ID
	loser.SupersededAt = &now
	loser.UpdatedAt = now
	loser.EmbeddingDim = nil // vector is purged below; keep row state in sync

	if err := p.memWriter.Update(ctx, loser); err != nil {
		slog.Warn("dreaming: paraphrase supersede update failed",
			"memory", loser.ID, "err", err)
		return
	}
	mirrorToStale(staleByID, loser)

	if p.vectorPurger != nil {
		if err := p.vectorPurger.Delete(ctx, storage.VectorKindMemory, loser.ID); err != nil {
			slog.Warn("dreaming: paraphrase vector purge failed",
				"memory", loser.ID, "err", err)
		}
	}

	_ = logger.LogOperation(ctx, model.DreamPhaseContradictions,
		model.DreamOpParaphraseSuperseded, "memory", loser.ID,
		nil, map[string]interface{}{
			"superseded_by": winner.ID.String(),
			"winner":        winnerSide,
			"cosine":        cosine,
			"reason":        "high_cosine_paraphrase",
		})
}

// stampContradictionsChecked records the visit stamp anchored to
// mem.UpdatedAt via UpdateMetadata so the staleness check
// (stamp < UpdatedAt) does not self-invalidate next cycle.
func (p *ContradictionPhase) stampContradictionsChecked(ctx context.Context, mem *model.Memory, meta map[string]interface{}) error {
	if meta == nil {
		meta = map[string]interface{}{}
	}
	meta[ContradictionsCheckedStampKey] = mem.UpdatedAt.UTC().Format(time.RFC3339Nano)

	encoded, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal contradiction stamp metadata: %w", err)
	}
	mem.Metadata = encoded
	if err := p.memWriter.UpdateMetadata(ctx, mem.ID, mem.NamespaceID, encoded); err != nil {
		return fmt.Errorf("persist contradiction stamp: %w", err)
	}
	return nil
}

func (p *ContradictionPhase) checkContradiction(
	ctx context.Context,
	llm provider.LLMProvider,
	a, b *model.Memory,
	prompt string,
	budget *TokenBudget,
) (bool, string, string, *provider.TokenUsage, error) {
	ctx = provider.WithOperation(ctx, provider.OperationDreamContradiction)
	ctx = provider.WithMemoryID(ctx, a.ID)
	resp, err := llm.Complete(ctx, &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   budget.PerCallCap(),
		Temperature: 0.1,
		JSONMode:    true,
	})
	if err != nil {
		return false, "", "", nil, err
	}

	usage := resp.Usage
	// Fall back to the 4-bytes-per-token heuristic when the provider omits
	// the usage field (e.g. Ollama's OpenAI-compat endpoint). Otherwise the
	// budget never advances and the cycle burns through every candidate.
	if usage.TotalTokens == 0 {
		if budget.MarkZeroUsageWarned() {
			slog.Warn("dreaming: provider returned zero token usage; estimating from prompt/response length",
				"provider", llm.Name(), "phase", model.DreamPhaseContradictions)
		}
		usage.PromptTokens = EstimateTokens(prompt)
		usage.CompletionTokens = EstimateTokens(resp.Content)
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	}

	// Winner is "a", "b", "tie", or empty when the operator's prompt predates
	// the winner field. Empty is normalized to "tie" at the call site so
	// custom prompts in the field continue to work without an upgrade.
	var result struct {
		Contradicts bool   `json:"contradicts"`
		Winner      string `json:"winner"`
		Explanation string `json:"explanation"`
	}

	content := strings.TrimSpace(resp.Content)
	if err := json.Unmarshal([]byte(content), &result); err != nil {
		return false, "", "", &usage, fmt.Errorf("parse contradiction response: %w", err)
	}

	return result.Contradicts, result.Winner, result.Explanation, &usage, nil
}

