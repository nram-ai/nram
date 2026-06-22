package enrichment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ingestionSettings is the snapshot taken once per phase invocation so the
// five (or six) cascade resolutions are not repeated mid-flight, and so
// future per-batch caching can fold in without changing call sites.
type ingestionSettings struct {
	threshold    float64
	topK         int
	shadow       bool
	systemPrompt string // tunable static instruction (role, rules, output schema)
}

// ingestionDecisionResult is the in-memory product of one ingestion-decision
// phase invocation. Worker code stamps these onto the pendingJob so they flow
// into runEmbedBatch (parent embed reuse) and finalizeJob (decision apply +
// metadata stamp).
type ingestionDecisionResult struct {
	enabled         bool
	shadow          bool
	decision        string // IngestionOpAdd / Update / Delete / None / AddFallback
	shadowOp        string // when shadow=true and the LLM picked something other than ADD
	target          *uuid.UUID
	rationale       string
	matches         int
	topScore        float64
	parentEmbedding []float32
	embedUsage      *provider.TokenUsage
	embedProvName   string
	embedModel      string
	usage           *provider.TokenUsage
	model           string
	providerName    string
}

// runIngestionDecision is the first enrichment phase. On near-duplicate
// matches it asks an LLM judge to pick ADD / UPDATE / DELETE / NONE. Failure
// at any step (settings missing, embed error, LLM error, parse error) falls
// through to ADD-FALLBACK so a memory is never lost to an ingestion-side
// fault. Returns nil when the phase is disabled or its dependencies are not
// wired; the caller treats nil as "no ingestion phase ran".
func (wp *WorkerPool) runIngestionDecision(ctx context.Context, job *model.EnrichmentJob, mem *model.Memory) *ingestionDecisionResult {
	if wp.settings == nil || wp.deduplicator == nil || wp.embedProvider == nil {
		return nil
	}
	if !wp.settings.ResolveBool(ctx, service.SettingIngestionDecisionEnabled, "global") {
		return nil
	}
	// Re-judging an already-enriched memory would write a duplicate lineage
	// edge. Backfill jobs should never run through this phase.
	//
	// DREAM-RECURSION GUARD: the Origin==OriginDream clause is the
	// ingestion-side enforcement of the dream-of-dream cascade prevention
	// contract. Both clauses are load-bearing; either alone is sufficient.
	// Symmetric sites:
	//
	//   - internal/dreaming/phase_consolidation.go (synthMemory creation,
	//       "DREAM-RECURSION GUARD: first prong"; sets Origin=OriginDream
	//       and Enriched=true)
	//   - internal/dreaming/phase_consolidation.go (consolidate() candidate
	//       filter, "DREAM-RECURSION GUARD: second prong")
	//   - internal/enrichment/worker.go (WorkerPool.runPreEmbed skipFact /
	//       skipEntity)
	//
	// Contract enforcer: internal/dreaming/dream_recursion_guard_test.go
	// (TestDreamRecursionGuard_EndToEnd).
	if mem.Enriched || mem.IsDream() {
		return nil
	}

	cfg := wp.resolveIngestionSettings(ctx, mem.NamespaceID)
	res := &ingestionDecisionResult{enabled: true, shadow: cfg.shadow}

	ep := wp.embedProvider()
	if ep == nil {
		res.decision = IngestionOpAdd
		return res
	}

	embedResp, err := ep.Embed(provider.WithOperation(ctx, provider.OperationIngestionEmbedding), &provider.EmbeddingRequest{Input: []string{mem.Content}})
	if err != nil || embedResp == nil || len(embedResp.Embeddings) == 0 {
		slog.Error("enrichment: ingestion_decision embed", "job", job.ID, "err", err)
		res.decision = IngestionOpAddFallback
		return res
	}
	res.parentEmbedding = embedResp.Embeddings[0]
	usage := embedResp.Usage
	res.embedUsage = &usage
	res.embedProvName = ep.Name()
	res.embedModel = embedResp.Model

	matches, err := wp.deduplicator.FindNearMatches(ctx, res.parentEmbedding, mem.NamespaceID, cfg.topK, cfg.threshold, &mem.ID)
	if err != nil {
		slog.Error("enrichment: ingestion_decision dedup", "job", job.ID, "err", err)
		res.decision = IngestionOpAddFallback
		return res
	}
	res.matches = len(matches)
	if len(matches) > 0 {
		res.topScore = matches[0].Score
	}
	if len(matches) == 0 {
		res.decision = IngestionOpAdd
		wp.logIngestionDecision(job, mem, res)
		return res
	}

	llmFactory := wp.ingestionProvider
	if llmFactory == nil {
		llmFactory = wp.factProvider
	}
	llm := llmFactory()
	if llm == nil {
		res.decision = IngestionOpAddFallback
		wp.logIngestionDecision(job, mem, res)
		return res
	}

	// Model is left empty: the ingestion-decision provider slot supplies the
	// model (falling back to the fact provider's model when no dedicated slot
	// is set, per Registry.GetIngestionDecision).
	system := cfg.systemPrompt
	user := RenderIngestionUser(mem.Content, matches)
	req := &provider.CompletionRequest{
		Messages:    provider.BuildMessages(provider.GuardedSystem(system), user),
		MaxTokens:   wp.settings.ResolveIntWithDefault(ctx, service.SettingEnrichmentIngestionDecisionMaxTokens, "global"),
		Temperature: wp.settings.ResolveFloatWithDefault(ctx, service.SettingEnrichmentIngestionDecisionTemperature, "global"),
		JSONMode:    true,
	}

	start := time.Now()
	ingestionCtx := provider.WithOperation(ctx, provider.OperationIngestionDecision)
	resp, err := llm.Complete(ingestionCtx, req)
	llmLatency := time.Since(start)
	if err != nil {
		slog.Error("enrichment: ingestion_decision llm", "job", job.ID, "err", err, "llm_latency_ms", llmLatency.Milliseconds())
		res.decision = IngestionOpAddFallback
		wp.logIngestionDecision(job, mem, res)
		return res
	}
	res.usage = &resp.Usage
	res.model = resp.Model
	res.providerName = llm.Name()

	parsed, parseErr := parseIngestionDecision(resp.Content)
	if parseErr != nil {
		// A re-send only helps when sampling could yield a different response.
		// At temperature 0 the request is deterministic, so when the first
		// response was truncated at the token cap the retry would truncate
		// identically and fail to parse again: skip it and fall through to the
		// fallback, saving a guaranteed-wasted completion. At temperature > 0,
		// or when the finish reason is unknown, retry exactly as before.
		if req.Temperature == 0 && provider.IsTruncated(resp.FinishReason) {
			slog.Warn("enrichment: ingestion_decision parse (truncated at temp=0, deterministic retry skipped)",
				"job", job.ID, "finish_reason", resp.FinishReason, "llm_latency_ms", llmLatency.Milliseconds())
			res.decision = IngestionOpAddFallback
			wp.logIngestionDecision(job, mem, res)
			return res
		}
		retryStart := time.Now()
		resp, err = llm.Complete(ingestionCtx, req)
		llmLatency += time.Since(retryStart)
		if err == nil {
			res.usage.PromptTokens += resp.Usage.PromptTokens
			res.usage.CompletionTokens += resp.Usage.CompletionTokens
			res.usage.TotalTokens += resp.Usage.TotalTokens
			parsed, parseErr = parseIngestionDecision(resp.Content)
		}
		if err != nil || parseErr != nil {
			slog.Error("enrichment: ingestion_decision parse", "job", job.ID, "err", parseErr, "llm_err", err, "llm_latency_ms", llmLatency.Milliseconds())
			res.decision = IngestionOpAddFallback
			wp.logIngestionDecision(job, mem, res)
			return res
		}
	}

	op, target, ok := validateIngestionDecision(parsed, matches)
	if !ok {
		slog.Warn("enrichment: ingestion_decision invalid",
			"job", job.ID, "raw_op", parsed.Operation, "raw_target", parsed.TargetID)
		res.decision = IngestionOpAddFallback
		wp.logIngestionDecision(job, mem, res)
		return res
	}
	res.decision = op
	res.target = target
	res.rationale = truncate(parsed.Rationale,
		wp.settings.ResolveIntWithDefault(ctx, service.SettingEnrichmentIngestionRationaleMaxLen, "global"))

	// Shadow mode: log the would-be decision but treat it as ADD downstream
	// so no lineage edges or supersessions are written.
	if res.shadow && op != IngestionOpAdd {
		res.shadowOp = op
		res.decision = IngestionOpAdd
		res.target = nil
	}

	slog.Info("enrichment: ingestion_decision",
		"job", job.ID,
		"memory", mem.ID,
		"op", res.decision,
		"shadow_op", res.shadowOp,
		"shadow", res.shadow,
		"match_count", res.matches,
		"top_score", res.topScore,
		"target_id", uuidPtrString(res.target),
		"llm_latency_ms", llmLatency.Milliseconds())

	return res
}

// resolveIngestionSettings snapshots the five admin-tunable ingestion knobs
// so the rest of the phase reads from a local struct rather than re-issuing
// settings cascades. Bad operator values fall back to documented defaults.
// The threshold respects the namespace cascade (system → user → project) when
// a CascadeResolver is wired; without one it falls back to the system-level
// ingestion_decision.threshold (with the legacy dedup_threshold key as a
// secondary fallback inside the resolver).
func (wp *WorkerPool) resolveIngestionSettings(ctx context.Context, namespaceID uuid.UUID) ingestionSettings {
	cfg := ingestionSettings{threshold: 0.92, topK: 5}
	if wp.cascade != nil {
		cfg.threshold = wp.cascade.ResolveDedupThreshold(ctx, namespaceID)
	} else if v, err := wp.settings.ResolveFloat(ctx, service.SettingIngestionDecisionThreshold, "global"); err == nil && v > 0 && v <= 1 {
		cfg.threshold = v
	}
	if v, err := wp.settings.ResolveInt(ctx, service.SettingIngestionDecisionTopK, "global"); err == nil && v > 0 {
		cfg.topK = v
	}
	cfg.shadow = wp.settings.ResolveBool(ctx, service.SettingIngestionDecisionShadow, "global")
	cfg.systemPrompt = service.ResolveOrDefault(ctx, wp.settings, service.SettingIngestionDecisionSystemPrompt, "global")
	return cfg
}

// applyIngestion copies ingestion-decision state onto the pendingJob.
// Called from both branches of runPreEmbed (the short-circuit DELETE branch
// and the normal-return branch) so the bookkeeping stays in one place.
func (p *pendingJob) applyIngestion(res *ingestionDecisionResult) {
	if res == nil {
		return
	}
	p.parentEmbedding = res.parentEmbedding
	p.ingestionDecision = res.decision
	p.ingestionTarget = res.target
	p.ingestionRationale = res.rationale
	p.ingestionMatchN = res.matches
	p.ingestionTopScore = res.topScore
	p.ingestionShadowOp = res.shadowOp
	p.ingestionUsage = res.usage
	p.ingestionModel = res.model
	p.ingestionProvName = res.providerName
	p.ingestionEmbedUsage = res.embedUsage
	p.ingestionEmbedProv = res.embedProvName
	p.ingestionEmbedModel = res.embedModel
}

// parentEmbedFromPhase reports whether the ingestion-decision phase
// pre-computed an embedding for the parent memory; runEmbedBatch reuses it
// instead of issuing a second embed for the same content.
func (p *pendingJob) parentEmbedFromPhase() bool { return len(p.parentEmbedding) > 0 }

// shortCircuitDelete reports whether the LLM judge marked the new memory as
// redundant; pre-embed exits early, runEmbedBatch skips, and finalizeJob
// soft-deletes.
func (p *pendingJob) shortCircuitDelete() bool { return p.ingestionDecision == IngestionOpDelete }

// logIngestionDecision is a fallback logger used when the function returns
// before reaching the main slog.Info site (no matches, embed/dedup failures,
// fallback paths). Keeps the telemetry footprint consistent.
func (wp *WorkerPool) logIngestionDecision(job *model.EnrichmentJob, mem *model.Memory, res *ingestionDecisionResult) {
	slog.Info("enrichment: ingestion_decision",
		"job", job.ID,
		"memory", mem.ID,
		"op", res.decision,
		"shadow", res.shadow,
		"match_count", res.matches,
		"top_score", res.topScore)
}

// rawDecision is the wire shape the LLM is instructed to return. Loose
// pointer/string typing: validateIngestionDecision normalises.
type rawDecision struct {
	Operation string `json:"operation"`
	TargetID  string `json:"target_id"`
	Rationale string `json:"rationale"`
}

// parseIngestionDecision extracts the decision JSON from an LLM response.
// JSONMode is requested on the call so the body is generally valid JSON, but
// some providers wrap output in markdown fences when JSON mode is unsupported;
// strip a leading fence defensively via the shared helper, raw-first so valid
// JSON never passes through StripCodeFence.
func parseIngestionDecision(raw string) (*rawDecision, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("empty decision response")
	}
	var d rawDecision
	if err := service.UnmarshalJSONLenient(raw, &d); err != nil {
		preview := trimmed
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		return nil, fmt.Errorf("decision json: %w (preview: %q)", err, preview)
	}
	return &d, nil
}

// validateIngestionDecision normalises a parsed decision and verifies that
// any target_id refers to one of the candidates the LLM was shown. An
// UPDATE/DELETE without a known target is rejected so the LLM cannot make up
// IDs.
func validateIngestionDecision(d *rawDecision, matches []MemoryMatch) (string, *uuid.UUID, bool) {
	op := strings.ToUpper(strings.TrimSpace(d.Operation))

	var target *uuid.UUID
	if t := strings.TrimSpace(d.TargetID); t != "" && t != "null" {
		parsed, err := uuid.Parse(t)
		if err != nil {
			return "", nil, false
		}
		if !slices.ContainsFunc(matches, func(m MemoryMatch) bool { return m.ID == parsed }) {
			return "", nil, false
		}
		target = &parsed
	}

	switch op {
	case IngestionOpUpdate, IngestionOpDelete:
		if target == nil {
			return "", nil, false
		}
	case IngestionOpAdd, IngestionOpNone:
		// target_id is allowed but ignored for these ops.
		target = nil
	default:
		return "", nil, false
	}
	return op, target, true
}

// ingestionUserWrapper is the hardcoded dynamic-half template for the
// ingestion-decision phase: the new memory content followed by the candidate
// list. It is code, not a setting; the tunable instruction (role, rules, output
// schema) lives entirely in SettingIngestionDecisionSystemPrompt, sent as the
// system message.

// RenderIngestionUser formats the new memory content and candidate list into the
// user message for the ingestion-decision phase. Each candidate is rendered as
// `[N] id: <uuid>, created: <RFC3339>, content: <content>`. A nil/empty matches
// slice yields an empty candidate block (used by the admin test surface). The
// new memory and candidate block are nonce-fenced as untrusted data.
func RenderIngestionUser(content string, matches []MemoryMatch) string {
	var b strings.Builder
	for i, m := range matches {
		fmt.Fprintf(&b, "[%d] id: %s, created: %s, content: %s\n",
			i+1, m.ID, m.CreatedAt.UTC().Format(time.RFC3339), m.Content)
	}
	return provider.Fence("new_memory", content) + "\n\n" +
		provider.Fence("candidates", strings.TrimRight(b.String(), "\n"))
}

// truncate caps a string at n bytes without splitting a UTF-8 rune. Walks
// back from the byte cap to the previous rune boundary so the returned
// string is always valid UTF-8.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func uuidPtrString(p *uuid.UUID) string {
	if p == nil {
		return ""
	}
	return p.String()
}

// finalizeShortCircuitDelete is the finalize path for a job whose ingestion
// decision was DELETE: soft-delete the new memory (which also purges its
// vector via the repo) and record the ingestion-decision token usage.
func (wp *WorkerPool) finalizeShortCircuitDelete(ctx context.Context, p *pendingJob) error {
	if wp.memSoftDeleter == nil {
		// Safety: the phase should not have decided DELETE without a
		// deleter wired. Treat as a normal completion to avoid losing the
		// memory; an operator will see the unexpected state in metadata.
		if err := wp.stampIngestionUnderLock(ctx, p); err != nil {
			if failErr := wp.queue.Fail(ctx, p.job.ID, p.workerID, fmt.Sprintf("update memory: %v", err)); failErr != nil {
				logClaimLostOr(failErr, "enrichment: fail-mark after memory update fallback", "job", p.job.ID, "worker", p.workerID)
			}
			return fmt.Errorf("update memory: %w", err)
		}
		// token_usage rows for the ingestion-decision phase (LLM + embed)
		// are written by the UsageRecordingProvider middleware on every
		// wrapped Complete/Embed call.
		if err := wp.queue.Complete(ctx, p.job.ID, p.workerID); err != nil {
			if errors.Is(err, storage.ErrClaimLost) {
				slog.Info("enrichment: complete dropped: claim lost", "job", p.job.ID, "worker", p.workerID)
				return nil
			}
			return err
		}
		return nil
	}

	// Stamp metadata BEFORE soft-delete. The Update path filters
	// `deleted_at IS NULL`, so the stamp would be silently dropped if it
	// happened after the delete.
	if err := wp.stampIngestionUnderLock(ctx, p); err != nil {
		slog.Warn("enrichment: stamp ingestion metadata before soft-delete",
			"job", p.job.ID, "memory", p.mem.ID, "err", err)
	}

	if err := wp.memSoftDeleter.SoftDelete(ctx, p.mem.ID, p.mem.NamespaceID); err != nil {
		if failErr := wp.queue.Fail(ctx, p.job.ID, p.workerID, fmt.Sprintf("ingestion delete soft-delete: %v", err)); failErr != nil {
			logClaimLostOr(failErr, "enrichment: fail-mark after soft-delete", "job", p.job.ID, "worker", p.workerID)
		}
		return fmt.Errorf("ingestion delete soft-delete: %w", err)
	}
	slog.Info("enrichment: ingestion_decision_apply",
		"job", p.job.ID,
		"memory", p.mem.ID,
		"op", IngestionOpDelete,
		"target_id", uuidPtrString(p.ingestionTarget),
		"shadow_op", p.ingestionShadowOp)

	// token_usage rows for the ingestion-decision phase (LLM + embed) are
	// written by the UsageRecordingProvider middleware on every wrapped call.

	if err := wp.queue.Complete(ctx, p.job.ID, p.workerID); err != nil {
		if errors.Is(err, storage.ErrClaimLost) {
			slog.Info("enrichment: complete dropped: claim lost", "job", p.job.ID, "worker", p.workerID)
			return nil
		}
		return fmt.Errorf("complete job: %w", err)
	}
	return nil
}

// applyIngestionUpdate writes the supersedes lineage edge and marks the
// target memory superseded by the new one. Failures are logged but not
// propagated: the new memory is already enriched and useful even if the
// supersession bookkeeping is incomplete.
func (wp *WorkerPool) applyIngestionUpdate(ctx context.Context, p *pendingJob) {
	if wp.lineage == nil || wp.memUpdater == nil || wp.memories == nil {
		return
	}

	now := time.Now().UTC()
	target := *p.ingestionTarget

	// Lineage: child = the new memory, parent = the existing one.
	contextBytes, _ := json.Marshal(map[string]any{
		"source":               "ingestion_decision",
		"top_score":            p.ingestionTopScore,
		"rationale":            p.ingestionRationale,
		"shadow_op_suppressed": p.ingestionShadowOp != "",
	})
	lin := &model.MemoryLineage{
		ID:          uuid.New(),
		NamespaceID: p.mem.NamespaceID,
		MemoryID:    p.mem.ID,
		ParentID:    &target,
		Relation:    model.LineageSupersedes,
		Context:     contextBytes,
		CreatedAt:   now,
	}
	if err := wp.lineage.Create(ctx, lin); err != nil {
		slog.Error("enrichment: ingestion update lineage", "job", p.job.ID, "target", target, "err", err)
		return
	}

	// MarkSupersededBy's WHERE clause guards on existence, deleted_at,
	// and superseded_by; a missing or already-superseded target
	// surfaces as ErrConcurrentSupersede with no extra round-trip.
	if err := wp.memUpdater.MarkSupersededBy(ctx, target, p.mem.NamespaceID, p.mem.ID); err != nil {
		slog.Error("enrichment: ingestion update target", "job", p.job.ID, "target", target, "err", err)
		return
	}
	if wp.vectorStore != nil {
		if err := wp.vectorStore.Delete(ctx, storage.VectorKindMemory, target); err != nil {
			slog.Warn("enrichment: ingestion update vector purge failed",
				"job", p.job.ID, "target", target, "err", err)
		}
	}
}

// stampIngestionUnderLock runs the metadata merge inside MutateInLock so
// the read-modify-write happens atomically under the row's advisory lock,
// then propagates the freshly-persisted Metadata and UpdatedAt back into
// p.mem so downstream code in the same job sees the post-write state.
// Returns the error verbatim on failure (and leaves p.mem untouched).
func (wp *WorkerPool) stampIngestionUnderLock(ctx context.Context, p *pendingJob) error {
	fresh, err := wp.memUpdater.MutateInLock(ctx, p.mem.ID, p.mem.NamespaceID, func(mem *model.Memory) (bool, error) {
		stampIngestionMetadataOn(mem, p)
		mem.UpdatedAt = time.Now().UTC()
		return true, nil
	})
	if err != nil {
		return err
	}
	p.mem.Metadata = fresh.Metadata
	p.mem.UpdatedAt = fresh.UpdatedAt
	return nil
}

// stampIngestionMetadataOn merges the ingestion-decision keys onto the
// given memory's Metadata in place, preserving existing keys. Used by
// stampIngestionUnderLock under MutateInLock so the merge happens against
// the freshly re-read row rather than the job-start snapshot, closing the
// cross-worker lost-update window for the metadata column. finalizeJob's
// pre-MarkEnriched stamp on the in-memory p.mem.Metadata calls it
// directly with p.mem.
func stampIngestionMetadataOn(mem *model.Memory, p *pendingJob) {
	if p.ingestionDecision == "" {
		return
	}
	meta := map[string]any{}
	if len(mem.Metadata) > 0 {
		_ = json.Unmarshal(mem.Metadata, &meta)
		if meta == nil {
			meta = map[string]any{}
		}
	}
	meta[model.IngestionMetaDecision] = p.ingestionDecision
	meta[model.IngestionMetaDecisionAt] = time.Now().UTC().Format(time.RFC3339Nano)
	if p.ingestionTarget != nil {
		meta[model.IngestionMetaTargetID] = p.ingestionTarget.String()
	}
	if p.ingestionRationale != "" {
		meta[model.IngestionMetaRationale] = p.ingestionRationale
	}
	meta[model.IngestionMetaMatchCount] = p.ingestionMatchN
	meta[model.IngestionMetaTopScore] = p.ingestionTopScore
	if p.ingestionShadowOp != "" {
		meta[model.IngestionMetaShadowOp] = p.ingestionShadowOp
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return
	}
	mem.Metadata = encoded
}
