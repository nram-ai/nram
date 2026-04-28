package provider

import (
	"context"
	"errors"
	"sync"
	"testing"

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
func (s *stubEmbedding) Name() string       { return s.name }
func (s *stubEmbedding) Dimensions() []int  { return nil }

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

// resolverStub returns a fixed UsageContext for every namespace lookup.
type resolverStub struct {
	uc *model.UsageContext
}

func (r *resolverStub) ResolveUsageContext(ctx context.Context, ns uuid.UUID) (*model.UsageContext, error) {
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

func TestUsageRecordingLLM_ZeroTokenFallback(t *testing.T) {
	rec := &captureRecorder{}
	llm := &stubLLM{
		name: "ollama",
		resp: &CompletionResponse{
			Content: "this is the model output",
			Model:   "llama-3",
			Usage:   TokenUsage{}, // provider returned zero
		},
	}
	w := NewUsageRecordingLLM(llm, rec, nil)

	ctx := WithOperation(context.Background(), OperationEntityExtraction)
	_, err := w.Complete(ctx, &CompletionRequest{
		Model: "llama-3",
		Messages: []Message{
			{Role: "user", Content: "hello world"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.TokensInput == 0 {
		t.Errorf("expected non-zero estimated input tokens, got 0")
	}
	if got.TokensOutput == 0 {
		t.Errorf("expected non-zero estimated output tokens, got 0")
	}
}

func TestUsageRecordingLLM_ErrorPath(t *testing.T) {
	rec := &captureRecorder{}
	llm := &stubLLM{
		name: "openai",
		err:  ErrCircuitOpen,
	}
	w := NewUsageRecordingLLM(llm, rec, nil)

	ctx := WithOperation(context.Background(), OperationFactExtraction)
	_, err := w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row even on error")
	}
	if got.Success {
		t.Error("expected Success=false")
	}
	if got.ErrorCode == nil || *got.ErrorCode != "circuit_open" {
		t.Errorf("expected ErrorCode=circuit_open, got %v", got.ErrorCode)
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
	// No WithUsageContext — middleware must fall back to resolver lookup.

	_, _ = w.Complete(ctx, &CompletionRequest{Model: "gpt-4o"})

	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.OrgID == nil || *got.OrgID != orgID {
		t.Errorf("expected resolver-supplied OrgID, got %v", got.OrgID)
	}
}
