package dreaming

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

type zeroUsageLLM struct {
	calls atomic.Int32
}

func (z *zeroUsageLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	z.calls.Add(1)
	return &provider.CompletionResponse{
		Content: `{"contradicts": false, "explanation": ""}`,
		Model:   "local-model",
		// Intentionally zero usage — mimics Ollama's OpenAI-compat endpoint.
		Usage: provider.TokenUsage{},
	}, nil
}
func (z *zeroUsageLLM) Name() string     { return "ollama-test" }
func (z *zeroUsageLLM) Models() []string { return []string{"local-model"} }

type recordingTokenRecorder struct {
	records []*model.TokenUsage
}

func (r *recordingTokenRecorder) Record(_ context.Context, u *model.TokenUsage) error {
	cp := *u
	r.records = append(r.records, &cp)
	return nil
}

type stubLineageWriter struct{}

func (stubLineageWriter) Create(_ context.Context, _ *model.MemoryLineage) error { return nil }

type stubUsageCtxResolver struct{}

func (stubUsageCtxResolver) ResolveUsageContext(_ context.Context, _ uuid.UUID) (*model.UsageContext, error) {
	return &model.UsageContext{}, nil
}

type stubSettings struct{}

func (stubSettings) Resolve(_ context.Context, _ string, _ string) (string, error) {
	// Two %s slots matching the contradiction prompt template contract.
	return "Compare these two statements for contradiction:\n\nA: %s\nB: %s\n\nReturn JSON.", nil
}
func (stubSettings) ResolveFloat(_ context.Context, _ string, _ string) (float64, error) {
	return 0, nil
}
func (stubSettings) ResolveInt(_ context.Context, _ string, _ string) (int, error) {
	return 0, nil
}
func (stubSettings) ResolveBool(_ context.Context, _ string, _ string) bool { return false }

func makeMemories(n int) []model.Memory {
	out := make([]model.Memory, n)
	nsID := uuid.New()
	for i := 0; i < n; i++ {
		out[i] = model.Memory{
			ID:          uuid.New(),
			NamespaceID: nsID,
			Content:     "memory content " + uuid.New().String(),
		}
	}
	return out
}

// TestContradictionPhase_ZeroUsageBudgetAdvances verifies the critical fix:
// when the provider reports Usage{TotalTokens: 0} (Ollama behaviour), the
// phase falls back to the len(prompt)/4 estimate so the TokenBudget still
// advances and Exhausted() eventually trips.
func TestContradictionPhase_ZeroUsageBudgetAdvances(t *testing.T) {
	llm := &zeroUsageLLM{}
	recorder := &recordingTokenRecorder{}
	memories := makeMemories(10)
	reader := &fakeMemoryReader{list: memories}

	phase := NewContradictionPhase(
		reader,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		stubSettings{},
		recorder,
		stubUsageCtxResolver{},
	)

	budget := NewTokenBudget(500, 128) // small on purpose
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: memories[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if budget.Used() == 0 {
		t.Fatalf("budget.Used() must advance past 0 even with zero-usage responses, got %d", budget.Used())
	}
	if llm.calls.Load() == 0 {
		t.Fatal("expected at least one LLM call")
	}
	if len(recorder.records) == 0 {
		t.Fatal("expected token usage records to be written for dreaming phase")
	}
	for _, r := range recorder.records {
		if r.Operation != "dream_contradiction" {
			t.Errorf("unexpected operation %q on token record", r.Operation)
		}
		if r.TokensInput == 0 && r.TokensOutput == 0 {
			t.Error("token record has zero totals; estimate fallback should have populated them")
		}
	}
}

// TestContradictionPhase_PairCapEnforced verifies findCandidatePairs caps
// at maxContradictionPairs even with many input memories. Regression guard
// against the old off-by-several-pairs bug where the cap check ran after
// completing a full inner loop row.
func TestContradictionPhase_PairCapEnforced(t *testing.T) {
	phase := &ContradictionPhase{}
	pairs, truncated := phase.findCandidatePairs(makeMemories(200))
	if len(pairs) != maxContradictionPairs {
		t.Errorf("expected exactly %d pairs, got %d", maxContradictionPairs, len(pairs))
	}
	if !truncated {
		t.Error("expected truncated=true when input exceeds cap")
	}
}

// malformedResponseLLM returns an unparseable body so the contradiction
// phase exercises its parse-error path.
type malformedResponseLLM struct {
	calls atomic.Int32
}

func (m *malformedResponseLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	m.calls.Add(1)
	return &provider.CompletionResponse{
		Content: "sure thing boss — not json",
		Model:   "local-model",
		Usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30},
	}, nil
}
func (m *malformedResponseLLM) Name() string     { return "ollama-malformed" }
func (m *malformedResponseLLM) Models() []string { return []string{"local-model"} }

// TestContradictionPhase_ParseErrorStillAccountsUsage verifies that when the
// LLM call succeeds but the response body is unparseable, the budget still
// advances and a token_usage record is still written. Otherwise a chatty
// small model that can't emit valid JSON would burn calls for free.
func TestContradictionPhase_ParseErrorStillAccountsUsage(t *testing.T) {
	llm := &malformedResponseLLM{}
	recorder := &recordingTokenRecorder{}
	memories := makeMemories(6)
	reader := &fakeMemoryReader{list: memories}

	phase := NewContradictionPhase(
		reader,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		stubSettings{},
		recorder,
		stubUsageCtxResolver{},
	)

	budget := NewTokenBudget(10000, 2048)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: memories[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if llm.calls.Load() == 0 {
		t.Fatal("expected LLM calls to happen")
	}
	if budget.Used() == 0 {
		t.Fatalf("budget must advance on parse-error path, got used=%d", budget.Used())
	}
	if len(recorder.records) == 0 {
		t.Fatal("expected token_usage records even when responses fail to parse")
	}
	if int(llm.calls.Load()) != len(recorder.records) {
		t.Errorf("expected one record per call; got calls=%d records=%d", llm.calls.Load(), len(recorder.records))
	}
}

// TestContradictionPhase_PreflightStopsWhenBudgetTooSmall verifies the
// pre-flight CanAfford check prevents calls when the estimated cost exceeds
// remaining budget.
func TestContradictionPhase_PreflightStopsWhenBudgetTooSmall(t *testing.T) {
	llm := &zeroUsageLLM{}
	recorder := &recordingTokenRecorder{}
	memories := makeMemories(4)
	reader := &fakeMemoryReader{list: memories}

	phase := NewContradictionPhase(
		reader,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		stubSettings{},
		recorder,
		stubUsageCtxResolver{},
	)

	// PerCallCap alone exceeds total budget — every pre-flight check fails.
	budget := NewTokenBudget(10, 100)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: memories[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if llm.calls.Load() != 0 {
		t.Errorf("expected 0 LLM calls when pre-flight fails, got %d", llm.calls.Load())
	}
}
