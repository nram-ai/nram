package provider

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
)

// UsageRecorder is the storage-side interface the wrapping middleware uses
// to persist a token_usage row. internal/storage.TokenUsageRepo satisfies it.
type UsageRecorder interface {
	Record(ctx context.Context, u *model.TokenUsage) error
}

// TokenCounter receives every recorded token-usage event so an external
// metrics system (e.g. Prometheus) can aggregate the totals. The callback
// is invoked synchronously after each Record call; nil counters are
// ignored at the call site. Defined as a func type rather than an
// interface to avoid a circular import between internal/provider and
// internal/observability/metrics.
type TokenCounter func(providerName, operation string, tokens float64)

// UsageContextResolver resolves org/user/project from a namespace ID. The
// middleware uses this as a fallback when the caller did not stamp a
// resolved *model.UsageContext on the context.
type UsageContextResolver interface {
	ResolveUsageContext(ctx context.Context, namespaceID uuid.UUID) (*model.UsageContext, error)
}

// Error code enum for token_usage.error_code. Bounded so analytics rollups
// stay clean and so we never accidentally spill PII from raw error strings.
const (
	errCodeCircuitOpen   = "circuit_open"
	errCodeTimeout       = "timeout"
	errCodeContextCancel = "context_canceled"
	errCodeProviderError = "provider_error"
)

// classifyError maps a provider error to a bounded enum code. Keep the set
// small; analytics consumers should be able to enumerate it.
func classifyError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, ErrCircuitOpen):
		return errCodeCircuitOpen
	case errors.Is(err, context.DeadlineExceeded):
		return errCodeTimeout
	case errors.Is(err, context.Canceled):
		return errCodeContextCancel
	default:
		return errCodeProviderError
	}
}

// ---------------------------------------------------------------------------
// UsageRecordingLLM
// ---------------------------------------------------------------------------

// UsageRecordingLLM wraps an LLMProvider and writes a token_usage row for
// every Complete call (success or failure). Recording is best-effort:
// recorder errors are logged but never propagated to the caller.
type UsageRecordingLLM struct {
	inner    LLMProvider
	recorder UsageRecorder
	resolver UsageContextResolver
	counter  TokenCounter
}

// NewUsageRecordingLLM wraps inner so every Complete call lands a token_usage
// row. resolver is optional; if nil, the middleware relies on the caller
// stamping a *model.UsageContext on the context (preferred path).
func NewUsageRecordingLLM(inner LLMProvider, recorder UsageRecorder, resolver UsageContextResolver) *UsageRecordingLLM {
	return &UsageRecordingLLM{inner: inner, recorder: recorder, resolver: resolver}
}

// WithTokenCounter attaches a Prometheus-style token counter that fires on
// every Record. Returns the same wrapper for chaining at construction time.
func (u *UsageRecordingLLM) WithTokenCounter(c TokenCounter) *UsageRecordingLLM {
	u.counter = c
	return u
}

// Complete delegates to the wrapped provider and records token usage.
func (u *UsageRecordingLLM) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	start := time.Now()
	resp, err := u.inner.Complete(ctx, req)
	latency := int(time.Since(start).Milliseconds())
	u.record(ctx, req, resp, err, latency)
	return resp, err
}

// Name returns the underlying provider's name.
func (u *UsageRecordingLLM) Name() string { return u.inner.Name() }

// Models returns the underlying provider's model list.
func (u *UsageRecordingLLM) Models() []string { return u.inner.Models() }

func (u *UsageRecordingLLM) record(
	ctx context.Context,
	req *CompletionRequest,
	resp *CompletionResponse,
	callErr error,
	latencyMs int,
) {
	if u.recorder == nil {
		return
	}

	op := operationOrUnknown(ctx, u.inner.Name())

	var usage TokenUsage
	var modelName string
	if resp != nil {
		usage = resp.Usage
		modelName = resp.Model
	}
	if modelName == "" {
		modelName = req.Model
	}

	// Tokenizer fallback: only when the provider reported zero tokens AND
	// the call returned a response we can measure. Joining messages saves
	// N-1 tiktoken.Encode dispatches per multi-turn prompt. EstimateMessages
	// is the shared join-and-estimate, so a dreaming caller estimating the
	// same zero-usage request against its TokenBudget lands on the same number.
	if usage.PromptTokens == 0 && usage.CompletionTokens == 0 && resp != nil {
		usage.PromptTokens = EstimateMessages(modelName, req.Messages)
		usage.CompletionTokens = EstimateTokens(modelName, resp.Content)
	}

	// Fire the Prometheus counter BEFORE the synchronous DB write. A
	// panic or hang in u.recorder.Record must not drop the in-process
	// metric; the counter is the always-on observability signal; the DB
	// row is the durable best-effort audit.
	if u.counter != nil {
		u.counter(u.inner.Name(), string(op), float64(usage.PromptTokens+usage.CompletionTokens))
	}

	if skipUsageRecordErr(callErr) {
		return
	}

	recCtx, cancel := recordingContext(ctx)
	defer cancel()
	rec := buildUsageRow(recCtx, u.resolver, u.inner.Name(), modelName, op,
		usage, latencyMs, callErr)

	if err := u.recorder.Record(recCtx, rec); err != nil {
		slog.Warn("usage_recorder: record failed",
			"provider", u.inner.Name(), "operation", op, "err", err)
	}
}

// ---------------------------------------------------------------------------
// UsageRecordingEmbedding
// ---------------------------------------------------------------------------

// UsageRecordingEmbedding wraps an EmbeddingProvider and writes a token_usage
// row for every Embed call (success or failure).
type UsageRecordingEmbedding struct {
	inner    EmbeddingProvider
	recorder UsageRecorder
	resolver UsageContextResolver
	counter  TokenCounter
}

// NewUsageRecordingEmbedding wraps inner so every Embed call lands a
// token_usage row.
func NewUsageRecordingEmbedding(inner EmbeddingProvider, recorder UsageRecorder, resolver UsageContextResolver) *UsageRecordingEmbedding {
	return &UsageRecordingEmbedding{inner: inner, recorder: recorder, resolver: resolver}
}

// WithTokenCounter attaches a Prometheus-style token counter that fires on
// every Record. Returns the same wrapper for chaining at construction time.
func (u *UsageRecordingEmbedding) WithTokenCounter(c TokenCounter) *UsageRecordingEmbedding {
	u.counter = c
	return u
}

// Embed delegates to the wrapped provider and records token usage.
func (u *UsageRecordingEmbedding) Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	start := time.Now()
	resp, err := u.inner.Embed(ctx, req)
	latency := int(time.Since(start).Milliseconds())
	u.record(ctx, req, resp, err, latency)
	return resp, err
}

// Name returns the underlying provider's name.
func (u *UsageRecordingEmbedding) Name() string { return u.inner.Name() }

// Dimensions returns the underlying provider's supported dimensions.
func (u *UsageRecordingEmbedding) Dimensions() []int { return u.inner.Dimensions() }

func (u *UsageRecordingEmbedding) record(
	ctx context.Context,
	req *EmbeddingRequest,
	resp *EmbeddingResponse,
	callErr error,
	latencyMs int,
) {
	if u.recorder == nil {
		return
	}

	op := operationOrUnknown(ctx, u.inner.Name())

	var promptTokens int
	var modelName string
	if resp != nil {
		promptTokens = resp.Usage.PromptTokens
		modelName = resp.Model
	}
	if modelName == "" {
		modelName = req.Model
	}

	// Embedding APIs commonly omit usage (Ollama's embed endpoint always; some
	// OpenAI-compat servers sometimes). Fall back to tokenizer estimation for
	// the input strings when the response is present but reports zero.
	if promptTokens == 0 && resp != nil {
		for _, in := range req.Input {
			promptTokens += EstimateTokens(modelName, in)
		}
	}

	// Counter before Record; see UsageRecordingLLM.record for rationale.
	if u.counter != nil {
		u.counter(u.inner.Name(), string(op), float64(promptTokens))
	}

	if skipUsageRecordErr(callErr) {
		return
	}

	recCtx, cancel := recordingContext(ctx)
	defer cancel()
	// Embedding endpoints carry no prompt-cache buckets on any provider, so the
	// cache counts stay zero here rather than being threaded from resp.Usage.
	rec := buildUsageRow(recCtx, u.resolver, u.inner.Name(), modelName, op,
		TokenUsage{PromptTokens: promptTokens}, latencyMs, callErr)

	if err := u.recorder.Record(recCtx, rec); err != nil {
		slog.Warn("usage_recorder: record failed",
			"provider", u.inner.Name(), "operation", op, "err", err)
	}
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

// skipUsageRecordErr reports whether a failed provider call should NOT get a
// durable token_usage row. A circuit-open rejection made no upstream call (the
// breaker short-circuited before any network I/O), so writing one row per
// rejection is pure write amplification while a provider is down. The
// Prometheus counter still fires at each call site, so the rejection stays
// observable as a metric. Real timeouts/provider errors (an actual call with
// latency) are still recorded.
func skipUsageRecordErr(callErr error) bool {
	return callErr != nil && classifyError(callErr) == errCodeCircuitOpen
}

// recordingContext returns a context that preserves all stamped values
// (UsageContext, RequestID, NamespaceID, Operation, etc.) but drops the
// caller's deadline and cancellation, then bounds the write at 5s.
// Recording is best-effort by design: a slow or near-deadline upstream
// call must not poison the token_usage write that follows it.
func recordingContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

// operationOrUnknown returns the operation stamped on ctx, or
// OperationUnknown, and warns with a stack trace when one is missing so the
// gap can be tracked back to the call site.
func operationOrUnknown(ctx context.Context, providerName string) Operation {
	if op, ok := OperationFromContext(ctx); ok && op != "" {
		return op
	}
	slog.Warn("usage_recorder: operation missing from context",
		"provider", providerName, "stack", string(debug.Stack()))
	return OperationUnknown
}

// buildUsageRow assembles a model.TokenUsage from ctx + call result. It
// resolves UsageContext from ctx if present, falls back to a per-namespace
// resolver lookup if not, and tolerates the absence of both (writing a row
// with NULL ownership rather than dropping the call).
func buildUsageRow(
	ctx context.Context,
	resolver UsageContextResolver,
	providerName, modelName string,
	op Operation,
	usage TokenUsage,
	latencyMs int,
	callErr error,
) *model.TokenUsage {
	// Every recorded row from all three recorders funnels through here, which
	// makes this the only place the subset invariant can be checked against
	// real provider data rather than asserted in a comment. Warn rather than
	// clamp or reject: Record is best-effort by design, so a rejecting
	// constraint would discard the row and hide the evidence. A provider
	// reporting more cached tokens than prompt tokens is the provider lying,
	// and the operator needs to see that.
	if usage.CacheReadTokens+usage.CacheWriteTokens > usage.PromptTokens {
		slog.Warn("usage_recorder: cache tokens exceed prompt tokens; provider reported an inconsistent usage block",
			"provider", providerName, "model", modelName, "operation", string(op),
			"prompt_tokens", usage.PromptTokens,
			"cache_read_tokens", usage.CacheReadTokens,
			"cache_write_tokens", usage.CacheWriteTokens)
	}
	rec := &model.TokenUsage{
		ID:               uuid.New(),
		Operation:        string(op),
		Provider:         providerName,
		Model:            modelName,
		TokensInput:      usage.PromptTokens,
		TokensOutput:     usage.CompletionTokens,
		TokensCacheRead:  usage.CacheReadTokens,
		TokensCacheWrite: usage.CacheWriteTokens,
		LatencyMs:        &latencyMs,
		Success:          callErr == nil,
		MemoryID:         MemoryIDFromContext(ctx),
		APIKeyID:         APIKeyIDFromContext(ctx),
		CycleID:          CycleIDFromContext(ctx),
		CreatedAt:        time.Now().UTC(),
	}
	if reqID := RequestIDFromContext(ctx); reqID != "" {
		v := reqID
		rec.RequestID = &v
	}
	if callErr != nil {
		code := classifyError(callErr)
		rec.ErrorCode = &code
	}

	// Ownership: prefer pre-stamped UsageContext (no DB hit), else resolver.
	// mergeOwnership only fills fields the caller left nil, so a partial
	// context (e.g. user+project stamped, org nil) keeps its caller stamps
	// while missing fields are backfilled from the namespace resolver.
	if uc := UsageContextFromContext(ctx); uc != nil {
		mergeOwnership(rec, uc)
	}
	rec.NamespaceID = NamespaceIDFromContext(ctx)
	if needsResolverLookup(rec, resolver) {
		if uc, err := resolver.ResolveUsageContext(ctx, rec.NamespaceID); err == nil && uc != nil {
			mergeOwnership(rec, uc)
		}
	}
	return rec
}

// mergeOwnership fills each nil ownership field on rec from uc without
// clobbering a value the caller already stamped. This matters for
// shared-project paths where the resolver's owner identity can differ from
// the caller's stamped user/project.
func mergeOwnership(rec *model.TokenUsage, uc *model.UsageContext) {
	if rec.OrgID == nil {
		rec.OrgID = uc.OrgID
	}
	if rec.UserID == nil {
		rec.UserID = uc.UserID
	}
	if rec.ProjectID == nil {
		rec.ProjectID = uc.ProjectID
	}
}

// needsResolverLookup is true when the caller stamped a namespace, a resolver
// is available, and any ownership field is still nil. The middleware then
// performs a one-shot DB lookup to backfill the missing org/user/project via
// mergeOwnership. Firing on any nil (rather than only all-nil) means a partial
// context (e.g. user+project stamped, org nil) still has its org backfilled
// instead of writing a NULL org_id row that org-scoped analytics would drop.
// A fully stamped context skips the lookup entirely.
func needsResolverLookup(rec *model.TokenUsage, resolver UsageContextResolver) bool {
	if resolver == nil || rec.NamespaceID == uuid.Nil {
		return false
	}
	return rec.OrgID == nil || rec.UserID == nil || rec.ProjectID == nil
}
