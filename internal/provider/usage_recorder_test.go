package provider

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
)

// stubLLM is a minimal LLMProvider whose Complete behavior is fully
// controllable from each test, including the response and error.
type stubLLM struct {
	name string
	resp *CompletionResponse
	err  error
}

func (s *stubLLM) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	return s.resp, s.err
}
func (s *stubLLM) Name() string     { return s.name }
func (s *stubLLM) Models() []string { return nil }

// stubEmbedding mirrors stubLLM for the embedding side.
type stubEmbedding struct {
	name string
	resp *EmbeddingResponse
	err  error
}

func (s *stubEmbedding) Embed(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	return s.resp, s.err
}
func (s *stubEmbedding) Name() string      { return s.name }
func (s *stubEmbedding) Dimensions() []int { return nil }

// captureRecorder collects every TokenUsage row sent through Record so
// tests can assert on the exact persisted shape.
type captureRecorder struct {
	mu      sync.Mutex
	rows    []*model.TokenUsage
	failErr error
}

func (c *captureRecorder) Record(ctx context.Context, u *model.TokenUsage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = append(c.rows, u)
	return c.failErr
}

func (c *captureRecorder) last() *model.TokenUsage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.rows) == 0 {
		return nil
	}
	return c.rows[len(c.rows)-1]
}

// resolverStub returns a fixed UsageContext for every namespace lookup and
// counts how many times it was consulted, so tests can assert the middleware
// skips the DB lookup when the caller already stamped a full context.
type resolverStub struct {
	uc    *model.UsageContext
	calls int
}

func (r *resolverStub) ResolveUsageContext(ctx context.Context, ns uuid.UUID) (*model.UsageContext, error) {
	r.calls++
	return r.uc, nil
}

func TestUsageRecordingLLM_HappyPath(t *testing.T) {
	rec := &captureRecorder{}
	llm := &stubLLM{
		name: "openai",
		resp: &CompletionResponse{
			Content: "ok",
			Model:   "gpt-4o",
			Usage:   TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
		},
	}
	w := NewUsageRecordingLLM(llm, rec, nil)

	ctx := WithOperation(context.Background(), OperationFactExtraction)
	ctx = WithRequestID(ctx, "req-001")

	resp, err := w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.Content != "ok" {
		t.Fatalf("unexpected content: %q", resp.Content)
	}

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.Operation != string(OperationFactExtraction) {
		t.Errorf("operation: got %q want %q", got.Operation, OperationFactExtraction)
	}
	if got.Provider != "openai" {
		t.Errorf("provider: got %q", got.Provider)
	}
	if got.Model != "gpt-4o" {
		t.Errorf("model: got %q", got.Model)
	}
	if got.TokensInput != 100 || got.TokensOutput != 50 {
		t.Errorf("tokens: got in=%d out=%d", got.TokensInput, got.TokensOutput)
	}
	if !got.Success {
		t.Error("expected Success=true")
	}
	if got.ErrorCode != nil {
		t.Errorf("expected nil ErrorCode, got %v", *got.ErrorCode)
	}
	if got.RequestID == nil || *got.RequestID != "req-001" {
		t.Errorf("expected RequestID=req-001, got %v", got.RequestID)
	}
	if got.LatencyMs == nil {
		t.Error("expected LatencyMs to be populated")
	}
}

// TestUsageRecordingLLM_ZeroTokenFallback pins the tokenizer fallback: when the
// provider reports no usage, the recorder estimates the row itself, and it does
// so over EstimateMessages of the messages actually sent.
//
// The exact-value assertion is the point. A dreaming caller estimates the same
// zero-usage request against its TokenBudget through the same EstimateMessages,
// so if this side ever measures some other text the one request gets billed two
// different numbers. Asserting merely non-zero would not catch that.
//
// Note the separator itself is not observable through the tokenizer: cl100k
// encodes both "\n" and "\n\n" as a single token, so joining on either yields
// the same count. The join is unified for single-source-of-truth and for the
// operator-facing rendered_prompt preview, not to move token numbers.
func TestUsageRecordingLLM_ZeroTokenFallback(t *testing.T) {
	const modelName = "llama-3"
	const body = "this is the model output"

	cases := []struct {
		name string
		msgs []Message
	}{
		{
			name: "single message",
			msgs: []Message{{Role: "user", Content: "hello world"}},
		},
		{
			name: "guarded system and user halves",
			msgs: []Message{
				{Role: "system", Content: "system half"},
				{Role: "user", Content: "user half"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &captureRecorder{}
			llm := &stubLLM{
				name: "ollama",
				resp: &CompletionResponse{
					Content: body,
					Model:   modelName,
					Usage:   TokenUsage{}, // provider returned zero
				},
			}
			w := NewUsageRecordingLLM(llm, rec, nil)

			ctx := WithOperation(context.Background(), OperationEntityExtraction)
			if _, err := w.Complete(ctx, &CompletionRequest{Model: modelName, Messages: tc.msgs}); err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			got := rec.last()
			if got == nil {
				t.Fatal("expected a recorded row")
			}
			wantInput := EstimateMessages(modelName, tc.msgs)
			if got.TokensInput != wantInput {
				t.Errorf("TokensInput = %d, want %d (estimate over the sent messages)",
					got.TokensInput, wantInput)
			}
			wantOutput := EstimateTokens(modelName, body)
			if got.TokensOutput != wantOutput {
				t.Errorf("TokensOutput = %d, want %d", got.TokensOutput, wantOutput)
			}
			if wantInput == 0 || wantOutput == 0 {
				t.Fatal("fixture estimates to zero tokens; the assertions cannot distinguish a missing fallback")
			}
		})
	}

	t.Run("multi-message estimate is not a single half", func(t *testing.T) {
		// Guard against a vacuous pass: the joined estimate must differ from
		// either half alone, otherwise the exact-value assertion above could not
		// tell a full-prompt estimate apart from one that measured one message.
		msgs := cases[1].msgs
		joined := EstimateMessages(modelName, msgs)
		if joined == EstimateTokens(modelName, msgs[0].Content) ||
			joined == EstimateTokens(modelName, msgs[1].Content) {
			t.Fatal("joined estimate coincides with a single message; the assertion cannot detect measuring the wrong text")
		}
	})
}

// TestUsageRecordingLLM_CircuitOpenSkipsRow pins the write-amplification fix: a
// circuit-open rejection made no upstream call, so it must NOT write a durable
// token_usage row (one per rejected call during an outage was a major source of
// runaway DB writes). The error still propagates and the Prometheus counter
// still fires so the rejection stays observable.
func TestUsageRecordingLLM_CircuitOpenSkipsRow(t *testing.T) {
	rec := &captureRecorder{}
	var counted int
	llm := &stubLLM{name: "openai", err: ErrCircuitOpen}
	w := NewUsageRecordingLLM(llm, rec, nil).WithTokenCounter(
		func(_, _ string, _ float64) { counted++ })

	ctx := WithOperation(context.Background(), OperationFactExtraction)
	_, err := w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if got := rec.last(); got != nil {
		t.Fatalf("expected NO recorded row on circuit-open, got %+v", got)
	}
	if counted != 1 {
		t.Errorf("expected token counter to fire once on circuit-open, got %d", counted)
	}
}

// TestUsageRecordingLLM_RealErrorStillRecords guards the other side: a genuine
// provider error (an actual call with latency) must still be recorded, so the
// circuit-open skip does not blind operators to real upstream failures.
func TestUsageRecordingLLM_RealErrorStillRecords(t *testing.T) {
	rec := &captureRecorder{}
	llm := &stubLLM{name: "openai", err: errors.New("upstream 500")}
	w := NewUsageRecordingLLM(llm, rec, nil)

	ctx := WithOperation(context.Background(), OperationFactExtraction)
	_, err := w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})
	if err == nil {
		t.Fatal("expected error")
	}

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row for a real provider error")
	}
	if got.Success {
		t.Error("expected Success=false")
	}
	if got.ErrorCode == nil || *got.ErrorCode != "provider_error" {
		t.Errorf("expected ErrorCode=provider_error, got %v", got.ErrorCode)
	}
	if got.TokensInput != 0 || got.TokensOutput != 0 {
		t.Error("error path should record zero tokens (no estimation when no response)")
	}
}

func TestUsageRecordingLLM_OperationMissing(t *testing.T) {
	rec := &captureRecorder{}
	llm := &stubLLM{
		name: "openai",
		resp: &CompletionResponse{Model: "gpt-4o", Usage: TokenUsage{PromptTokens: 1}},
	}
	w := NewUsageRecordingLLM(llm, rec, nil)

	_, _ = w.Complete(context.Background(), &CompletionRequest{Model: "gpt-4o"})

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.Operation != string(OperationUnknown) {
		t.Errorf("expected Operation=unknown when ctx unstamped, got %q", got.Operation)
	}
}

func TestUsageRecordingEmbedding_ZeroTokenFallback(t *testing.T) {
	rec := &captureRecorder{}
	emb := &stubEmbedding{
		name: "ollama",
		resp: &EmbeddingResponse{
			Embeddings: [][]float32{{0.1, 0.2}},
			Model:      "nomic-embed-text",
			Usage:      TokenUsage{},
		},
	}
	w := NewUsageRecordingEmbedding(emb, rec, nil)

	ctx := WithOperation(context.Background(), OperationEmbedding)
	_, err := w.Embed(ctx, &EmbeddingRequest{
		Model: "nomic-embed-text",
		Input: []string{"hello world embedding input"},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.TokensInput == 0 {
		t.Error("expected estimated input tokens, got 0")
	}
	if got.TokensOutput != 0 {
		t.Errorf("embedding output tokens must always be 0, got %d", got.TokensOutput)
	}
}

func TestUsageRecordingLLM_RecorderErrorDoesNotPropagate(t *testing.T) {
	rec := &captureRecorder{failErr: errors.New("db down")}
	llm := &stubLLM{
		name: "openai",
		resp: &CompletionResponse{Model: "gpt-4o", Usage: TokenUsage{PromptTokens: 5}},
	}
	w := NewUsageRecordingLLM(llm, rec, nil)

	ctx := WithOperation(context.Background(), OperationEmbedding)
	_, err := w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})
	if err != nil {
		t.Fatalf("recorder failure must not propagate, got %v", err)
	}
}

func TestUsageRecordingLLM_UsageContextFromCtx(t *testing.T) {
	rec := &captureRecorder{}
	llm := &stubLLM{
		name: "openai",
		resp: &CompletionResponse{Model: "gpt-4o", Usage: TokenUsage{PromptTokens: 5}},
	}
	w := NewUsageRecordingLLM(llm, rec, nil)

	orgID := uuid.New()
	userID := uuid.New()
	projectID := uuid.New()
	ns := uuid.New()
	mem := uuid.New()
	apiKey := uuid.New()

	ctx := WithOperation(context.Background(), OperationFactExtraction)
	ctx = WithUsageContext(ctx, &model.UsageContext{
		OrgID: &orgID, UserID: &userID, ProjectID: &projectID,
	})
	ctx = WithNamespaceID(ctx, ns)
	ctx = WithMemoryID(ctx, mem)
	ctx = WithAPIKeyID(ctx, &apiKey)

	_, _ = w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.OrgID == nil || *got.OrgID != orgID {
		t.Errorf("OrgID: got %v want %v", got.OrgID, orgID)
	}
	if got.UserID == nil || *got.UserID != userID {
		t.Errorf("UserID mismatch")
	}
	if got.ProjectID == nil || *got.ProjectID != projectID {
		t.Errorf("ProjectID mismatch")
	}
	if got.NamespaceID != ns {
		t.Errorf("NamespaceID: got %v want %v", got.NamespaceID, ns)
	}
	if got.MemoryID == nil || *got.MemoryID != mem {
		t.Errorf("MemoryID mismatch")
	}
	if got.APIKeyID == nil || *got.APIKeyID != apiKey {
		t.Errorf("APIKeyID mismatch")
	}
}

func TestUsageRecordingLLM_FallbackResolver(t *testing.T) {
	rec := &captureRecorder{}
	orgID := uuid.New()
	userID := uuid.New()
	projectID := uuid.New()
	resolver := &resolverStub{
		uc: &model.UsageContext{OrgID: &orgID, UserID: &userID, ProjectID: &projectID},
	}
	llm := &stubLLM{
		name: "openai",
		resp: &CompletionResponse{Model: "gpt-4o", Usage: TokenUsage{PromptTokens: 5}},
	}
	w := NewUsageRecordingLLM(llm, rec, resolver)

	ns := uuid.New()
	ctx := WithOperation(context.Background(), OperationFactExtraction)
	ctx = WithNamespaceID(ctx, ns)
	// No WithUsageContext; middleware must fall back to resolver lookup.

	_, _ = w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.OrgID == nil || *got.OrgID != orgID {
		t.Errorf("expected resolver-supplied OrgID, got %v", got.OrgID)
	}
}

// TestUsageRecordingLLM_PartialContextBackfillsOrg covers the partial-context
// case: the caller stamped user+project but no org (as an org user's request
// that failed to thread OrgID would). The resolver must backfill the org while
// the caller's user/project stamps are preserved (not clobbered by the
// resolver's owner identity).
func TestUsageRecordingLLM_PartialContextBackfillsOrg(t *testing.T) {
	rec := &captureRecorder{}
	resolverOrg := uuid.New()
	resolverUser := uuid.New()
	resolverProject := uuid.New()
	resolver := &resolverStub{
		uc: &model.UsageContext{OrgID: &resolverOrg, UserID: &resolverUser, ProjectID: &resolverProject},
	}
	llm := &stubLLM{
		name: "openai",
		resp: &CompletionResponse{Model: "gpt-4o", Usage: TokenUsage{PromptTokens: 5}},
	}
	w := NewUsageRecordingLLM(llm, rec, resolver)

	callerUser := uuid.New()
	callerProject := uuid.New()
	ns := uuid.New()
	ctx := WithOperation(context.Background(), OperationEmbedding)
	// Partial: user+project stamped, org nil.
	ctx = WithUsageContext(ctx, &model.UsageContext{UserID: &callerUser, ProjectID: &callerProject})
	ctx = WithNamespaceID(ctx, ns)

	_, _ = w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if resolver.calls != 1 {
		t.Errorf("expected exactly one resolver lookup, got %d", resolver.calls)
	}
	if got.OrgID == nil || *got.OrgID != resolverOrg {
		t.Errorf("OrgID: got %v want resolver-supplied %v", got.OrgID, resolverOrg)
	}
	if got.UserID == nil || *got.UserID != callerUser {
		t.Errorf("UserID: got %v want caller stamp %v (must not be clobbered)", got.UserID, callerUser)
	}
	if got.ProjectID == nil || *got.ProjectID != callerProject {
		t.Errorf("ProjectID: got %v want caller stamp %v (must not be clobbered)", got.ProjectID, callerProject)
	}
}

// TestUsageRecordingLLM_FullContextSkipsResolver verifies a fully stamped
// context passes through unchanged and never triggers a resolver DB lookup.
func TestUsageRecordingLLM_FullContextSkipsResolver(t *testing.T) {
	rec := &captureRecorder{}
	otherOrg := uuid.New()
	resolver := &resolverStub{
		uc: &model.UsageContext{OrgID: &otherOrg, UserID: &otherOrg, ProjectID: &otherOrg},
	}
	llm := &stubLLM{
		name: "openai",
		resp: &CompletionResponse{Model: "gpt-4o", Usage: TokenUsage{PromptTokens: 5}},
	}
	w := NewUsageRecordingLLM(llm, rec, resolver)

	orgID := uuid.New()
	userID := uuid.New()
	projectID := uuid.New()
	ns := uuid.New()
	ctx := WithOperation(context.Background(), OperationFactExtraction)
	ctx = WithUsageContext(ctx, &model.UsageContext{OrgID: &orgID, UserID: &userID, ProjectID: &projectID})
	ctx = WithNamespaceID(ctx, ns)

	_, _ = w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if resolver.calls != 0 {
		t.Errorf("expected no resolver lookup for a full context, got %d", resolver.calls)
	}
	if got.OrgID == nil || *got.OrgID != orgID {
		t.Errorf("OrgID: got %v want %v", got.OrgID, orgID)
	}
	if got.UserID == nil || *got.UserID != userID {
		t.Errorf("UserID mismatch")
	}
	if got.ProjectID == nil || *got.ProjectID != projectID {
		t.Errorf("ProjectID mismatch")
	}
}

// recordCtxSnapshot is what ctxCaptureRecorder records: a snapshot of
// the recording context taken inline at Record time, since the caller's
// deferred cancel() fires the moment record() returns.
type recordCtxSnapshot struct {
	row    *model.TokenUsage
	err    error
	op     Operation
	hasDdl bool
}

type ctxCaptureRecorder struct {
	mu        sync.Mutex
	snapshots []recordCtxSnapshot
}

func (c *ctxCaptureRecorder) Record(ctx context.Context, u *model.TokenUsage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	op, _ := OperationFromContext(ctx)
	_, hasDdl := ctx.Deadline()
	c.snapshots = append(c.snapshots, recordCtxSnapshot{
		row:    u,
		err:    ctx.Err(),
		op:     op,
		hasDdl: hasDdl,
	})
	return nil
}

// TestUsageRecordingEmbedding_RecordSurvivesExpiredCallerCtx is the
// load-bearing assertion that the recorder is decoupled from the
// upstream call's deadline. The embedder's Embed runs to completion;
// when record() runs afterwards, the caller's ctx is already expired.
// The row must still be written with a live recording context.
func TestUsageRecordingEmbedding_RecordSurvivesExpiredCallerCtx(t *testing.T) {
	rec := &ctxCaptureRecorder{}
	emb := &stubEmbedding{
		name: "openai",
		resp: &EmbeddingResponse{
			Embeddings: [][]float32{{1, 2, 3}},
			Model:      "text-embedding-3-small",
			Usage:      TokenUsage{PromptTokens: 5},
		},
	}
	w := NewUsageRecordingEmbedding(emb, rec, nil)

	ctx, cancel := context.WithDeadline(
		WithOperation(context.Background(), OperationEmbedding),
		time.Now().Add(-1*time.Hour),
	)
	defer cancel()

	_, _ = w.Embed(ctx, &EmbeddingRequest{Input: []string{"x"}})

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.snapshots) != 1 {
		t.Fatalf("expected exactly 1 recorded row, got %d", len(rec.snapshots))
	}
	got := rec.snapshots[0]
	if got.err != nil {
		t.Fatalf("recorder context must be live at Record time; got Err()=%v", got.err)
	}
	if !got.hasDdl {
		t.Errorf("recording context should carry its own bounded deadline (5s)")
	}
	// WithoutCancel preserves Value lookups; the operation must survive.
	if got.op != OperationEmbedding {
		t.Errorf("recording context lost stamped operation; got op=%q", got.op)
	}
}
