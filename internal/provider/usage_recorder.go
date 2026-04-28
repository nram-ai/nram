package provider

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
)

// UsageRecorder is the storage-side interface the wrapping middleware uses
// to persist a token_usage row. internal/storage.TokenUsageRepo satisfies it.
type UsageRecorder interface {
	Record(ctx context.Context, u *model.TokenUsage) error
}

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
// small — analytics consumers should be able to enumerate it.
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
}

// NewUsageRecordingLLM wraps inner so every Complete call lands a token_usage
// row. resolver is optional; if nil, the middleware relies on the caller
// stamping a *model.UsageContext on the context (preferred path).
func NewUsageRecordingLLM(inner LLMProvider, recorder UsageRecorder, resolver UsageContextResolver) *UsageRecordingLLM {
	return &UsageRecordingLLM{inner: inner, recorder: recorder, resolver: resolver}
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

	var promptTokens, completionTokens int
	var modelName string
	if resp != nil {
		promptTokens = resp.Usage.PromptTokens
		completionTokens = resp.Usage.CompletionTokens
		modelName = resp.Model
	}
	if modelName == "" {
		modelName = req.Model
	}

	// Tokenizer fallback: only when the provider reported zero tokens AND
	// the call returned a response we can measure. Joining messages saves
	// N-1 tiktoken.Encode dispatches per multi-turn prompt.
	if promptTokens == 0 && completionTokens == 0 && resp != nil {
		promptTokens = EstimateTokens(modelName, joinMessages(req.Messages))
		completionTokens = EstimateTokens(modelName, resp.Content)
	}

	rec := buildUsageRow(ctx, u.resolver, u.inner.Name(), modelName, op,
		promptTokens, completionTokens, latencyMs, callErr)

	if err := u.recorder.Record(ctx, rec); err != nil {
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
}

// NewUsageRecordingEmbedding wraps inner so every Embed call lands a
// token_usage row.
func NewUsageRecordingEmbedding(inner EmbeddingProvider, recorder UsageRecorder, resolver UsageContextResolver) *UsageRecordingEmbedding {
	return &UsageRecordingEmbedding{inner: inner, recorder: recorder, resolver: resolver}
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

	rec := buildUsageRow(ctx, u.resolver, u.inner.Name(), modelName, op,
		promptTokens, 0, latencyMs, callErr)

	if err := u.recorder.Record(ctx, rec); err != nil {
		slog.Warn("usage_recorder: record failed",
			"provider", u.inner.Name(), "operation", op, "err", err)
	}
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

// operationOrUnknown returns the operation stamped on ctx, or
// OperationUnknown — and warns with a stack trace when one is missing so the
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
	tokensIn, tokensOut, latencyMs int,
	callErr error,
) *model.TokenUsage {
	rec := &model.TokenUsage{
		ID:           uuid.New(),
		Operation:    string(op),
		Provider:     providerName,
		Model:        modelName,
		TokensInput:  tokensIn,
		TokensOutput: tokensOut,
		LatencyMs:    &latencyMs,
		Success:      callErr == nil,
		MemoryID:     MemoryIDFromContext(ctx),
		APIKeyID:     APIKeyIDFromContext(ctx),
		CreatedAt:    time.Now().UTC(),
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
	if uc := UsageContextFromContext(ctx); uc != nil {
		rec.OrgID = uc.OrgID
		rec.UserID = uc.UserID
		rec.ProjectID = uc.ProjectID
	}
	rec.NamespaceID = NamespaceIDFromContext(ctx)
	if needsResolverLookup(rec, resolver) {
		if uc, err := resolver.ResolveUsageContext(ctx, rec.NamespaceID); err == nil && uc != nil {
			rec.OrgID = uc.OrgID
			rec.UserID = uc.UserID
			rec.ProjectID = uc.ProjectID
		}
	}
	return rec
}

// needsResolverLookup is true when the caller stamped a namespace but no
// UsageContext, and a resolver is available. The middleware then performs
// a one-shot DB lookup to populate org/user/project; without all four
// preconditions the row is recorded with whatever ownership data ctx
// carries (possibly NULL).
func needsResolverLookup(rec *model.TokenUsage, resolver UsageContextResolver) bool {
	if resolver == nil || rec.NamespaceID == uuid.Nil {
		return false
	}
	return rec.OrgID == nil && rec.UserID == nil && rec.ProjectID == nil
}

// joinMessages concatenates the message contents into a single string for
// the tokenizer fallback. Joining with a separator prevents word-boundary
// merges from skewing the token count downward; "\n" matches the natural
// turn boundary in chat-style requests.
func joinMessages(msgs []Message) string {
	if len(msgs) == 0 {
		return ""
	}
	if len(msgs) == 1 {
		return msgs[0].Content
	}
	parts := make([]string, len(msgs))
	for i, m := range msgs {
		parts[i] = m.Content
	}
	return strings.Join(parts, "\n")
}
