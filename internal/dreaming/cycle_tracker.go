package dreaming

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/provider"
)

// InFlightCall describes a single LLM call currently in progress for a cycle.
// Snapshotted by the heartbeat goroutine and serialized into
// dream.cycle.heartbeat events.
type InFlightCall struct {
	CallID    uuid.UUID `json:"call_id"`
	Operation string    `json:"operation"`
	Model     string    `json:"model,omitempty"`
	TargetID  string    `json:"target_id,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// CycleTracker emits per-cycle progress events on the event bus and exposes a
// Snapshot of the in-flight LLM call so the runner's heartbeat goroutine can
// publish liveness updates without coordinating with the phase that owns the
// call. One CycleTracker is constructed per cycle, in Runner.Execute.
type CycleTracker struct {
	bus       events.EventBus
	cycleID   uuid.UUID
	projectID uuid.UUID
	scope     string

	currentCall  atomic.Pointer[InFlightCall]
	currentPhase atomic.Pointer[string]
}

// NewCycleTracker returns a tracker bound to a single cycle. bus may be nil,
// in which case all emit calls are no-ops; this matches the soft-fail
// discipline the rest of the dreaming pipeline uses for event delivery.
func NewCycleTracker(bus events.EventBus, cycleID, projectID uuid.UUID) *CycleTracker {
	return &CycleTracker{
		bus:       bus,
		cycleID:   cycleID,
		projectID: projectID,
		scope:     "project:" + projectID.String(),
	}
}

// Snapshot returns the in-flight call descriptor or nil if no call is
// currently outstanding. The returned pointer is owned by the caller.
func (t *CycleTracker) Snapshot() *InFlightCall {
	p := t.currentCall.Load()
	if p == nil {
		return nil
	}
	c := *p
	return &c
}

// Phase returns the current phase name, or empty if not set.
func (t *CycleTracker) Phase() string {
	p := t.currentPhase.Load()
	if p == nil {
		return ""
	}
	return *p
}

// SetPhase updates the current phase name. Called by the runner just before
// invoking phase.Execute.
func (t *CycleTracker) SetPhase(phase string) {
	t.currentPhase.Store(&phase)
}

// CycleID returns the cycle ID.
func (t *CycleTracker) CycleID() uuid.UUID { return t.cycleID }

// ProjectID returns the project ID.
func (t *CycleTracker) ProjectID() uuid.UUID { return t.projectID }

// Scope returns the event scope used for emissions ("project:<uuid>").
func (t *CycleTracker) Scope() string { return t.scope }

// EmitPhaseStarted publishes dream.phase.started.
func (t *CycleTracker) EmitPhaseStarted(ctx context.Context, phase string, tokensUsed int) {
	if t == nil {
		return
	}
	events.Emit(ctx, t.bus, events.DreamPhaseStarted, t.scope, map[string]any{
		"cycle_id":    t.cycleID.String(),
		"project_id":  t.projectID.String(),
		"phase":       phase,
		"tokens_used": tokensUsed,
	})
}

// EmitPhaseCompleted publishes dream.phase.completed. errStr is the empty
// string on success.
func (t *CycleTracker) EmitPhaseCompleted(
	ctx context.Context,
	phase string,
	tokensUsed, operations int,
	durationMs int64,
	hasResidual bool,
	errStr string,
) {
	if t == nil {
		return
	}
	payload := map[string]any{
		"cycle_id":     t.cycleID.String(),
		"project_id":   t.projectID.String(),
		"phase":        phase,
		"tokens_used":  tokensUsed,
		"operations":   operations,
		"duration_ms":  durationMs,
		"has_residual": hasResidual,
		"ok":           errStr == "",
	}
	if errStr != "" {
		payload["error"] = errStr
	}
	events.Emit(ctx, t.bus, events.DreamPhaseCompleted, t.scope, payload)
}

// usageOrEstimateLLM returns the usage struct from an LLM response,
// substituting EstimateTokens for prompt and completion when the provider
// reports zero (Ollama's OpenAI-compat endpoint, some local proxies). Without
// the fallback the dream-cycle TokenBudget never advances and the cycle
// burns through every candidate. budget.MarkZeroUsageWarned dedups the
// per-cycle warning so logs stay clean.
func usageOrEstimateLLM(resp *provider.CompletionResponse, prompt string, budget *TokenBudget, providerName, phase string) *provider.TokenUsage {
	if resp == nil {
		return nil
	}
	u := resp.Usage
	if u.TotalTokens == 0 {
		if budget != nil && budget.MarkZeroUsageWarned() {
			slog.Warn("dreaming: provider returned zero token usage; estimating from prompt/response length",
				"provider", providerName, "phase", phase)
		}
		u.PromptTokens = EstimateTokens(prompt)
		u.CompletionTokens = EstimateTokens(resp.Content)
		u.TotalTokens = u.PromptTokens + u.CompletionTokens
	}
	return &u
}

// usageOrEstimateEmbed returns the usage struct from an embedding response,
// substituting EstimateTokens summed over the inputs when the provider
// reports zero. See usageOrEstimateLLM for why the fallback exists.
func usageOrEstimateEmbed(resp *provider.EmbeddingResponse, inputs []string) *provider.TokenUsage {
	if resp == nil {
		return nil
	}
	u := resp.Usage
	if u.TotalTokens == 0 {
		est := 0
		for _, s := range inputs {
			est += EstimateTokens(s)
		}
		u.PromptTokens = est
		u.TotalTokens = est
	}
	return &u
}

// SSE call-operation labels. These are the dream.call.{started,completed}
// "operation" payload field — they classify each LLM/embedder round trip in
// the admin UI's live timeline and are referenced from the phase code below
// and from the React handler. Adding a new wrapped call site means adding a
// constant here so typos fail at compile time.
const (
	OpContradictionJudge      = "contradiction_judge"
	OpContradictionEmbedProbe = "contradiction_embed_probe"
	OpContradictionEmbedBatch = "contradiction_embed_batch"
	OpAlignmentScore          = "alignment_score"
	OpSynthesis               = "synthesis"
	OpNoveltyAuditEmbed       = "novelty_audit_embed"
	OpNoveltyAuditLLM         = "novelty_audit_llm"
	OpParaphraseEmbedProbe    = "paraphrase_embed_probe"
	OpEmbedBackfill           = "embed_backfill"
)

// progressEmitStep returns the iteration interval at which silent phases
// should emit dream.phase.progress. Caps total ticks at ≤ 40 per phase
// regardless of work size so the SSE stream stays readable on cycles with
// millions of items. Uses ceiling division so total / step is bounded above
// by 40 — floor division would overshoot when total is not a multiple of 40.
func progressEmitStep(total int) int {
	if total <= 40 {
		return 1
	}
	return (total + 39) / 40
}

// shouldEmitProgress returns true when the i'th iteration (0-indexed) of a
// total-element loop should emit dream.phase.progress. Always fires at the
// final iteration so the UI sees the boundary, plus every step iterations
// in between. Place at the top of the silent-phase loop — emitting after
// continue branches starves progress on phases that skip most rows.
func shouldEmitProgress(i, total, step int) bool {
	if total <= 0 {
		return false
	}
	return i+1 == total || (i+1)%step == 0
}

// EmitPhaseProgress publishes dream.phase.progress for phases whose work is
// dominated by SQL-only iteration (entity dedup, transitive inference,
// pruning, weight adjustment). Without this signal those phases look frozen
// to the admin UI between dream.phase.{started,completed} markers.
//
// label describes what is being counted ("memories", "entities",
// "relationships", "weights") so the UI can render a meaningful chip.
func (t *CycleTracker) EmitPhaseProgress(ctx context.Context, current, total int, label string) {
	if t == nil {
		return
	}
	events.Emit(ctx, t.bus, events.DreamPhaseProgress, t.scope, map[string]any{
		"cycle_id":   t.cycleID.String(),
		"project_id": t.projectID.String(),
		"phase":      t.Phase(),
		"current":    current,
		"total":      total,
		"label":      label,
		"timestamp":  time.Now().UTC(),
	})
}

// EmitHeartbeat publishes dream.cycle.heartbeat carrying current phase, tokens
// used, and the in-flight call (if any). Called from the runner's heartbeat
// goroutine after the database heartbeat write.
func (t *CycleTracker) EmitHeartbeat(ctx context.Context, tokensUsed int) {
	if t == nil {
		return
	}
	payload := map[string]any{
		"cycle_id":    t.cycleID.String(),
		"project_id":  t.projectID.String(),
		"phase":       t.Phase(),
		"tokens_used": tokensUsed,
		"timestamp":   time.Now().UTC(),
	}
	if call := t.Snapshot(); call != nil {
		payload["in_flight_call"] = map[string]any{
			"call_id":    call.CallID.String(),
			"operation":  call.Operation,
			"model":      call.Model,
			"target_id":  call.TargetID,
			"started_at": call.StartedAt,
			"elapsed_ms": time.Since(call.StartedAt).Milliseconds(),
		}
	}
	events.Emit(ctx, t.bus, events.DreamCycleHeartbeat, t.scope, payload)
}

// trackerCtxKey is the context key used to bind a CycleTracker to a context.
// Phases pull the tracker out of context so the Phase.Execute interface
// signature can stay backward-compatible (matches the provider.WithMemoryID
// and provider.WithOperation patterns already in this codebase).
type trackerCtxKey struct{}

// WithCycleTracker returns a child context carrying t. The returned context
// is consumed by WrapLLMCall in phases.
func WithCycleTracker(ctx context.Context, t *CycleTracker) context.Context {
	if t == nil {
		return ctx
	}
	return context.WithValue(ctx, trackerCtxKey{}, t)
}

// CycleTrackerFromContext returns the tracker bound to ctx, or nil if none.
func CycleTrackerFromContext(ctx context.Context) *CycleTracker {
	if ctx == nil {
		return nil
	}
	t, _ := ctx.Value(trackerCtxKey{}).(*CycleTracker)
	return t
}

// WrapLLMCall is the single LLM/embedder call path the dreaming pipeline
// knows about. It instruments the call (emits dream.call.{started,completed}
// when a tracker is bound) AND charges the cycle's TokenBudget when usage is
// returned. Phases never call budget.Spend directly — wrapping is the only
// way LLM/embedder tokens reach the budget, which guarantees that every
// wrapped call site charges and the budget can never be silently bypassed
// by a future phase author.
//
// Generic over the call's primary return type so call sites read naturally:
//
//	alignment, usage, err := dreaming.WrapLLMCall(ctx, budget, "alignment_score",
//	    model, syn.ID.String(),
//	    func(ctx context.Context) (float64, *provider.TokenUsage, error) {
//	        return p.scoreAlignment(ctx, llm, prompt)
//	    })
//
// budget may be nil (unit tests, embedder probes outside a cycle) — the
// Spend step is skipped. usage may be non-nil even on call error (the LLM
// call already happened); the budget is still charged in that case so
// reporting reflects real spend. If budget.Spend returns ErrBudgetExhausted
// AND fn itself returned nil, that error surfaces to the caller so the
// runner can break out of the phase loop on the next ctx check.
func WrapLLMCall[T any](
	ctx context.Context,
	budget *TokenBudget,
	operation, model, targetID string,
	fn func(ctx context.Context) (T, *provider.TokenUsage, error),
) (T, *provider.TokenUsage, error) {
	spend := func(usage *provider.TokenUsage, callErr error) error {
		if usage == nil || budget == nil {
			return callErr
		}
		if spendErr := budget.Spend(usage.TotalTokens); spendErr != nil && callErr == nil {
			return spendErr
		}
		return callErr
	}

	t := CycleTrackerFromContext(ctx)
	if t == nil {
		result, usage, err := fn(ctx)
		return result, usage, spend(usage, err)
	}

	call := &InFlightCall{
		CallID:    uuid.New(),
		Operation: operation,
		Model:     model,
		TargetID:  targetID,
		StartedAt: time.Now().UTC(),
	}
	emitStart(ctx, t, call)
	t.setInFlight(call)
	defer t.clearInFlight(call)

	result, usage, err := fn(ctx)
	err = spend(usage, err)
	emitComplete(ctx, t, call, usage, err)
	return result, usage, err
}

func (t *CycleTracker) setInFlight(c *InFlightCall) {
	if t == nil {
		return
	}
	t.currentCall.Store(c)
}

// clearInFlight only clears if the current pointer matches c. Phases run
// calls serially today, but the CAS keeps Snapshot() honest if two
// WrapLLMCall invocations ever overlap on the same tracker.
func (t *CycleTracker) clearInFlight(c *InFlightCall) {
	if t == nil {
		return
	}
	t.currentCall.CompareAndSwap(c, nil)
}

func emitStart(ctx context.Context, t *CycleTracker, c *InFlightCall) {
	if t == nil {
		return
	}
	events.Emit(ctx, t.bus, events.DreamCallStarted, t.scope, map[string]any{
		"cycle_id":   t.cycleID.String(),
		"project_id": t.projectID.String(),
		"call_id":    c.CallID.String(),
		"operation":  c.Operation,
		"model":      c.Model,
		"target_id":  c.TargetID,
		"phase":      t.Phase(),
		"started_at": c.StartedAt,
	})
}

func emitComplete(ctx context.Context, t *CycleTracker, c *InFlightCall, usage *provider.TokenUsage, err error) {
	if t == nil {
		return
	}
	payload := map[string]any{
		"cycle_id":   t.cycleID.String(),
		"project_id": t.projectID.String(),
		"call_id":    c.CallID.String(),
		"operation":  c.Operation,
		"phase":      t.Phase(),
		"started_at": c.StartedAt,
		"ended_at":   time.Now().UTC(),
		"latency_ms": time.Since(c.StartedAt).Milliseconds(),
		"ok":         err == nil,
	}
	if usage != nil {
		payload["tokens"] = map[string]int{
			"prompt":     usage.PromptTokens,
			"completion": usage.CompletionTokens,
			"total":      usage.TotalTokens,
		}
	}
	if err != nil {
		payload["error"] = err.Error()
	}
	events.Emit(ctx, t.bus, events.DreamCallCompleted, t.scope, payload)
}
