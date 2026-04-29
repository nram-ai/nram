package enrichment

import (
	"context"
	"encoding/json"
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
	threshold float64
	topK      int
	shadow    bool
	model     string
	prompt    string
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

// rationaleMaxLen caps the rationale stored on memory metadata so the JSONB
// column does not balloon if the LLM returns prose.
const rationaleMaxLen = 500

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
	if mem.Enriched {
		return nil
	}

	cfg := wp.resolveIngestionSettings(ctx)
	res := &ingestionDecisionResult{enabled: true, shadow: cfg.shadow}

	ep := wp.embedProvider()
	if ep == nil {
		res.decision = IngestionOpAdd
		return res
	}

	embedResp, err := ep.Embed(provider.WithOperation(ctx, provider.OperationEmbedding), &provider.EmbeddingRequest{Input: []string{mem.Content}})
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

	req := &provider.CompletionRequest{
		Messages:    []provider.Message{{Role: "user", Content: renderIngestionPrompt(cfg.prompt, cfg.topK, mem.Content, matches)}},
		Model:       cfg.model,
		MaxTokens:   512,
		Temperature: 0.0,
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
	res.rationale = truncate(parsed.Rationale, rationaleMaxLen)

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
func (wp *WorkerPool) resolveIngestionSettings(ctx context.Context) ingestionSettings {
	cfg := ingestionSettings{threshold: 0.92, topK: 5}
	if v, err := wp.settings.ResolveFloat(ctx, service.SettingIngestionDecisionThreshold, "global"); err == nil && v > 0 && v <= 1 {
		cfg.threshold = v
	}
	if v, err := wp.settings.ResolveInt(ctx, service.SettingIngestionDecisionTopK, "global"); err == nil && v > 0 {
		cfg.topK = v
	}
	cfg.shadow = wp.settings.ResolveBool(ctx, service.SettingIngestionDecisionShadow, "global")
	cfg.model, _ = wp.settings.Resolve(ctx, service.SettingIngestionDecisionModel, "global")
	cfg.prompt, _ = wp.settings.Resolve(ctx, service.SettingIngestionDecisionPrompt, "global")
	if cfg.prompt == "" {
		cfg.prompt, _ = service.GetDefault(service.SettingIngestionDecisionPrompt)
	}
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
// strip a leading fence defensively.
func parseIngestionDecision(raw string) (*rawDecision, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty decision response")
	}
	var d rawDecision
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		preview := raw
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

// renderIngestionPrompt formats the candidate list into the prompt template.
// Each candidate is rendered as `[N] id: <uuid>, created: <RFC3339>, content:
// <content>`. The template is the configured prompt body which carries the
// instruction header and the strict-JSON output schema.
func renderIngestionPrompt(template string, topK int, content string, matches []MemoryMatch) string {
	var b strings.Builder
	for i, m := range matches {
		fmt.Fprintf(&b, "[%d] id: %s, created: %s, content: %s\n",
			i+1, m.ID, m.CreatedAt.UTC().Format(time.RFC3339), m.Content)
	}
	return fmt.Sprintf(template, topK, content, b.String())
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
		stampIngestionMetadata(p)
		p.mem.UpdatedAt = time.Now().UTC()
		if err := wp.memUpdater.Update(ctx, p.mem); err != nil {
			_ = wp.queue.Fail(ctx, p.job.ID, fmt.Sprintf("update memory: %v", err))
			return fmt.Errorf("update memory: %w", err)
		}
		// token_usage rows for the ingestion-decision phase (LLM + embed)
		// are written by the UsageRecordingProvider middleware on every
		// wrapped Complete/Embed call.
		return wp.queue.Complete(ctx, p.job.ID)
	}

	// Stamp metadata BEFORE soft-delete. The Update path filters
	// `deleted_at IS NULL`, so the stamp would be silently dropped if it
	// happened after the delete.
	stampIngestionMetadata(p)
	p.mem.UpdatedAt = time.Now().UTC()
	if err := wp.memUpdater.Update(ctx, p.mem); err != nil {
		slog.Warn("enrichment: stamp ingestion metadata before soft-delete",
			"job", p.job.ID, "memory", p.mem.ID, "err", err)
	}

	if err := wp.memSoftDeleter.SoftDelete(ctx, p.mem.ID, p.mem.NamespaceID); err != nil {
		_ = wp.queue.Fail(ctx, p.job.ID, fmt.Sprintf("ingestion delete soft-delete: %v", err))
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

	if err := wp.queue.Complete(ctx, p.job.ID); err != nil {
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
	contextBytes, _ := json.Marshal(map[string]interface{}{
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

	// Mark the target memory superseded by the new one and drop its
	// vector. embedding_dim is cleared so the row state matches.
	targetMem, err := wp.memories.GetByID(ctx, target)
	if err != nil {
		slog.Error("enrichment: ingestion update load target", "job", p.job.ID, "target", target, "err", err)
		return
	}
	newID := p.mem.ID
	targetMem.SupersededBy = &newID
	targetMem.SupersededAt = &now
	targetMem.UpdatedAt = now
	targetMem.EmbeddingDim = nil
	if err := wp.memUpdater.Update(ctx, targetMem); err != nil {
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

// stampIngestionMetadata writes ingestion-decision fields onto the parent
// memory's `metadata` JSONB column so admin views and downstream consumers
// can see what happened. Existing metadata keys are preserved.
func stampIngestionMetadata(p *pendingJob) {
	if p.ingestionDecision == "" {
		return
	}
	meta := map[string]interface{}{}
	if len(p.mem.Metadata) > 0 {
		_ = json.Unmarshal(p.mem.Metadata, &meta)
		if meta == nil {
			meta = map[string]interface{}{}
		}
	}
	meta["ingestion_decision"] = p.ingestionDecision
	meta["ingestion_decision_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	if p.ingestionTarget != nil {
		meta["ingestion_target_id"] = p.ingestionTarget.String()
	}
	if p.ingestionRationale != "" {
		meta["ingestion_rationale"] = p.ingestionRationale
	}
	meta["ingestion_match_count"] = p.ingestionMatchN
	meta["ingestion_top_score"] = p.ingestionTopScore
	if p.ingestionShadowOp != "" {
		meta["ingestion_shadow_op"] = p.ingestionShadowOp
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return
	}
	p.mem.Metadata = encoded
}
