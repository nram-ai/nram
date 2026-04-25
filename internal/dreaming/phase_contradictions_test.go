package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
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
func (stubLineageWriter) CountConflictsBetween(_ context.Context, _, _, _ uuid.UUID) (int, error) {
	return 0, nil
}

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

func stampedMemories(n int, stamp, updatedAt time.Time) []model.Memory {
	mems := makeMemories(n)
	for i := range mems {
		meta := map[string]interface{}{
			ContradictionsCheckedStampKey: stamp.Format(time.RFC3339Nano),
		}
		raw, _ := json.Marshal(meta)
		mems[i].Metadata = raw
		mems[i].UpdatedAt = updatedAt
	}
	return mems
}

func nilEmbedder() provider.EmbeddingProvider { return nil }

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
		&updatingMemoryWriter{},
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		nilEmbedder,
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
		&updatingMemoryWriter{},
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		nilEmbedder,
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
	// One record per successful LLM call; recorder may also carry a
	// dream_contradiction_embedding row when an embedder is wired, so count
	// only the dream_contradiction entries here.
	contradictionRecords := 0
	for _, r := range recorder.records {
		if r.Operation == "dream_contradiction" {
			contradictionRecords++
		}
	}
	if int(llm.calls.Load()) != contradictionRecords {
		t.Errorf("expected one dream_contradiction record per LLM call; got calls=%d records=%d",
			llm.calls.Load(), contradictionRecords)
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
		&updatingMemoryWriter{},
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		nilEmbedder,
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

// TestContradictionPhase_NoStaleReturnsResidualFalse is the primary
// acceptance test for the stop-signal fix. All memories carry a stamp
// newer than UpdatedAt, so the phase must short-circuit to residual=false
// with zero LLM, Update, or embedder calls.
func TestContradictionPhase_NoStaleReturnsResidualFalse(t *testing.T) {
	now := time.Now().UTC()
	mems := stampedMemories(20, now, now.Add(-time.Minute))
	reader := &fakeMemoryReader{list: mems}
	writer := &updatingMemoryWriter{}
	llm := &zeroUsageLLM{}
	emb := &staticEmbedder{}
	recorder := &recordingTokenRecorder{}

	phase := NewContradictionPhase(
		reader,
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		stubSettings{},
		recorder,
		stubUsageCtxResolver{},
	)

	budget := NewTokenBudget(10000, 2048)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	residual, err := phase.Execute(context.Background(), cycle, budget, logger)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if residual {
		t.Fatal("expected residual=false when no memories are stale")
	}
	if llm.calls.Load() != 0 {
		t.Errorf("expected 0 LLM calls, got %d", llm.calls.Load())
	}
	if emb.calls.Load() != 0 {
		t.Errorf("expected 0 embedder calls when no stale memories, got %d", emb.calls.Load())
	}
	if len(writer.updates) != 0 {
		t.Errorf("expected 0 Update calls, got %d", len(writer.updates))
	}
}

// TestContradictionPhase_StampsDispatchedAndReportsResidualWhenCapHit
// verifies that when more stale memories exist than the cap can fully
// cover, the phase stamps the subset it dispatched and reports residual=true.
func TestContradictionPhase_StampsDispatchedAndReportsResidualWhenCapHit(t *testing.T) {
	llm := &zeroUsageLLM{}
	recorder := &recordingTokenRecorder{}
	mems := makeMemories(100)
	reader := &fakeMemoryReader{list: mems}
	writer := &updatingMemoryWriter{}

	phase := NewContradictionPhase(
		reader,
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		nilEmbedder,
		stubSettings{}, // cap defaults to 30, K defaults to 4
		recorder,
		stubUsageCtxResolver{},
	)

	budget := NewTokenBudget(1_000_000, 2048)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	residual, err := phase.Execute(context.Background(), cycle, budget, logger)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !residual {
		t.Fatal("expected residual=true when stale memories exceed cap")
	}
	if int(llm.calls.Load()) > 30 {
		t.Errorf("cap=30 was violated: %d calls", llm.calls.Load())
	}
	if len(writer.updates) == 0 {
		t.Fatal("expected some memories to be stamped")
	}
	if len(writer.updates) >= len(mems) {
		t.Errorf("expected fewer stamps than total memories; got %d of %d", len(writer.updates), len(mems))
	}
	for _, u := range writer.updates {
		meta := map[string]interface{}{}
		if err := json.Unmarshal(u.Metadata, &meta); err != nil {
			t.Fatalf("stamped memory has unparseable metadata: %v", err)
		}
		if _, ok := meta[ContradictionsCheckedStampKey]; !ok {
			t.Errorf("stamped memory missing %q in metadata", ContradictionsCheckedStampKey)
		}
	}
}

// TestContradictionPhase_UpdatedAtInvalidatesStamp verifies that when a
// memory's UpdatedAt moves past its contradictions_checked_at stamp, it is
// considered stale and gets re-processed.
func TestContradictionPhase_UpdatedAtInvalidatesStamp(t *testing.T) {
	now := time.Now().UTC()
	// Most memories are fresh (stamp newer than UpdatedAt).
	mems := stampedMemories(10, now, now.Add(-time.Hour))
	// One memory has an older stamp than its UpdatedAt.
	staleStamp := now.Add(-2 * time.Hour)
	freshUpdated := now.Add(-time.Minute)
	meta := map[string]interface{}{
		ContradictionsCheckedStampKey: staleStamp.Format(time.RFC3339Nano),
	}
	raw, _ := json.Marshal(meta)
	mems[0].Metadata = raw
	mems[0].UpdatedAt = freshUpdated

	reader := &fakeMemoryReader{list: mems}
	writer := &updatingMemoryWriter{}
	llm := &zeroUsageLLM{}
	recorder := &recordingTokenRecorder{}

	phase := NewContradictionPhase(
		reader,
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		nilEmbedder,
		stubSettings{},
		recorder,
		stubUsageCtxResolver{},
	)

	budget := NewTokenBudget(100_000, 2048)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	residual, err := phase.Execute(context.Background(), cycle, budget, logger)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if residual {
		t.Error("expected residual=false after draining the single stale memory")
	}
	if len(writer.updates) != 1 {
		t.Errorf("expected exactly 1 stamp update, got %d", len(writer.updates))
	}
	if len(writer.updates) > 0 && writer.updates[0].ID != mems[0].ID {
		t.Errorf("expected the stale memory to be stamped, got %s", writer.updates[0].ID)
	}
}

// TestContradictionPhase_StampingIsIdempotent is the forward-progress
// acceptance criterion: once the phase drains to residual=false, a
// subsequent pass on the same input must be a complete no-op. Drains may
// take multiple passes when the stale set exceeds cap*K — the invariant
// is that stability, once reached, is preserved.
func TestContradictionPhase_StampingIsIdempotent(t *testing.T) {
	llm := &zeroUsageLLM{}
	recorder := &recordingTokenRecorder{}
	mems := makeMemories(10) // all stale
	store := &mutableMemoryStore{memories: mems}

	phase := NewContradictionPhase(
		store,
		store,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		nilEmbedder,
		stubSettings{},
		recorder,
		stubUsageCtxResolver{},
	)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	// Drain to stability. Must terminate within a bounded number of passes.
	const maxDrainPasses = 8
	var drainedAt int
	for pass := 0; pass < maxDrainPasses; pass++ {
		residual, err := phase.Execute(context.Background(), cycle,
			NewTokenBudget(1_000_000, 2048), logger)
		if err != nil {
			t.Fatalf("drain pass %d: %v", pass, err)
		}
		if !residual {
			drainedAt = pass
			break
		}
		if pass == maxDrainPasses-1 {
			t.Fatalf("phase failed to drain in %d passes", maxDrainPasses)
		}
	}
	if llm.calls.Load() == 0 {
		t.Fatal("drain should have made LLM calls")
	}
	if store.updateCount() == 0 {
		t.Fatal("drain should have stamped memories")
	}

	callsAfterDrain := llm.calls.Load()
	updatesAfterDrain := store.updateCount()

	// Verify idempotency: another cycle on the drained state is a no-op.
	residual, err := phase.Execute(context.Background(), cycle,
		NewTokenBudget(1_000_000, 2048), logger)
	if err != nil {
		t.Fatalf("post-drain pass: %v", err)
	}
	if residual {
		t.Error("post-drain pass expected residual=false")
	}
	if llm.calls.Load() != callsAfterDrain {
		t.Errorf("post-drain pass should be LLM no-op; additional calls = %d",
			llm.calls.Load()-callsAfterDrain)
	}
	if store.updateCount() != updatesAfterDrain {
		t.Errorf("post-drain pass should be Update no-op; additional updates = %d",
			store.updateCount()-updatesAfterDrain)
	}
	t.Logf("drained in %d passes; %d LLM calls, %d stamps",
		drainedAt+1, callsAfterDrain, updatesAfterDrain)
}

// TestIsStale_StampEqualsUpdatedAt_NotStale is the strict-less-than
// invariant: a just-stamped memory (stamp == UpdatedAt) must not look stale
// on the next cycle, or stamping becomes self-defeating.
func TestIsStale_StampEqualsUpdatedAt_NotStale(t *testing.T) {
	now := time.Now().UTC()
	meta := map[string]interface{}{
		ContradictionsCheckedStampKey: now.Format(time.RFC3339Nano),
	}
	mem := model.Memory{UpdatedAt: now}

	if isStale(&mem, meta) {
		t.Fatal("stamp time == UpdatedAt must be considered fresh")
	}
}

// TestContradictionPhase_TooFewMemoriesIsNoOp confirms the guard at the top
// of Execute short-circuits when the namespace can't produce any pair.
func TestContradictionPhase_TooFewMemoriesIsNoOp(t *testing.T) {
	for _, n := range []int{0, 1} {
		llm := &zeroUsageLLM{}
		writer := &updatingMemoryWriter{}
		mems := makeMemories(n)
		reader := &fakeMemoryReader{list: mems}

		phase := NewContradictionPhase(
			reader,
			writer,
			stubLineageWriter{},
			func() provider.LLMProvider { return llm },
			nilEmbedder,
			stubSettings{},
			&recordingTokenRecorder{},
			stubUsageCtxResolver{},
		)

		ns := uuid.New()
		if n > 0 {
			ns = mems[0].NamespaceID
		}
		residual, err := phase.Execute(
			context.Background(),
			&model.DreamCycle{ID: uuid.New(), NamespaceID: ns},
			NewTokenBudget(10000, 2048),
			NewDreamLogWriter(nil, uuid.New(), uuid.UUID{}),
		)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if residual {
			t.Errorf("n=%d: expected residual=false", n)
		}
		if llm.calls.Load() != 0 {
			t.Errorf("n=%d: expected 0 LLM calls", n)
		}
		if len(writer.updates) != 0 {
			t.Errorf("n=%d: expected 0 Update calls", n)
		}
	}
}

// TestContradictionPhase_EmbedderNilDegradesSafely confirms the phase still
// terminates and stamps memories when no embedder is available.
func TestContradictionPhase_EmbedderNilDegradesSafely(t *testing.T) {
	llm := &zeroUsageLLM{}
	writer := &updatingMemoryWriter{}
	mems := makeMemories(8)
	reader := &fakeMemoryReader{list: mems}

	phase := NewContradictionPhase(
		reader,
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		nilEmbedder,
		stubSettings{},
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)

	budget := NewTokenBudget(1_000_000, 2048)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	residual, err := phase.Execute(context.Background(), cycle, budget, logger)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	// 8 memories * K=4 = 32 candidate pairs but dedup reduces that; cap=30
	// may or may not be hit depending on overlap. Either way, Update must
	// have been called on at least one memory (the degradation path must
	// not abort stamping).
	if len(writer.updates) == 0 {
		t.Fatal("expected degradation path to still stamp memories")
	}
	if residual && len(writer.updates) == len(mems) {
		t.Error("residual=true with every memory stamped is contradictory")
	}
	if llm.calls.Load() == 0 {
		t.Fatal("expected LLM calls from the degradation path")
	}
}

// erroringEmbedder simulates a failing embedder so the phase exercises its
// embedder-error degradation path.
type erroringEmbedder struct {
	calls atomic.Int32
}

func (e *erroringEmbedder) Embed(_ context.Context, _ *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	e.calls.Add(1)
	return nil, errors.New("embedder offline")
}
func (e *erroringEmbedder) Name() string       { return "erroring" }
func (e *erroringEmbedder) Dimensions() []int  { return []int{64} }

// TestContradictionPhase_EmbedderErrorDegradesSafely confirms that an
// embedder error falls back to the deterministic walk rather than aborting
// the cycle, and still stamps visited memories.
func TestContradictionPhase_EmbedderErrorDegradesSafely(t *testing.T) {
	llm := &zeroUsageLLM{}
	writer := &updatingMemoryWriter{}
	emb := &erroringEmbedder{}
	mems := makeMemories(8)
	reader := &fakeMemoryReader{list: mems}

	phase := NewContradictionPhase(
		reader,
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		stubSettings{},
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)

	budget := NewTokenBudget(1_000_000, 2048)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if emb.calls.Load() == 0 {
		t.Fatal("expected the embedder to be called once")
	}
	if len(writer.updates) == 0 {
		t.Fatal("expected stamping to still happen on embedder error")
	}
	if llm.calls.Load() == 0 {
		t.Fatal("expected LLM calls via the degradation path")
	}
}

// mutableMemoryStore is a combined reader+writer that applies Update
// mutations back into the in-memory list so cycle-to-cycle idempotency
// tests can observe persisted stamps on subsequent reads.
type mutableMemoryStore struct {
	memories []model.Memory
	updates  int
}

func (m *mutableMemoryStore) updateCount() int { return m.updates }

func (m *mutableMemoryStore) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for i := range m.memories {
		if m.memories[i].ID == id {
			return &m.memories[i], nil
		}
	}
	return nil, errors.New("not found")
}
func (m *mutableMemoryStore) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	want := map[uuid.UUID]struct{}{}
	for _, id := range ids {
		want[id] = struct{}{}
	}
	out := []model.Memory{}
	for i := range m.memories {
		if _, ok := want[m.memories[i].ID]; ok {
			out = append(out, m.memories[i])
		}
	}
	return out, nil
}
func (m *mutableMemoryStore) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return m.memories, nil
}
func (m *mutableMemoryStore) CountByNamespace(_ context.Context, _ uuid.UUID) (int, error) {
	return len(m.memories), nil
}
func (m *mutableMemoryStore) Create(_ context.Context, mem *model.Memory) error {
	m.memories = append(m.memories, *mem)
	return nil
}
func (m *mutableMemoryStore) Update(_ context.Context, mem *model.Memory) error {
	for i := range m.memories {
		if m.memories[i].ID == mem.ID {
			m.memories[i] = *mem
			m.updates++
			return nil
		}
	}
	return errors.New("not found")
}
func (m *mutableMemoryStore) SoftDelete(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}
func (m *mutableMemoryStore) HardDelete(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}
func (m *mutableMemoryStore) DecayConfidence(_ context.Context, ids []uuid.UUID, _, _ float64) (int64, error) {
	return int64(len(ids)), nil
}

// --- haircut/winner tests (item #5) ---

// decidingLLM emits a structured contradiction response with a configurable
// winner field. Used to drive the haircut path through the full code branches
// (WinnerSideA, WinnerSideB, WinnerTie, or "" for legacy-prompt fallback).
type decidingLLM struct {
	winner string
	calls  atomic.Int32
}

func (d *decidingLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	d.calls.Add(1)
	body := `{"contradicts": true, "explanation": "test"}`
	if d.winner != "" {
		body = `{"contradicts": true, "winner": "` + d.winner + `", "explanation": "test"}`
	}
	return &provider.CompletionResponse{
		Content: body,
		Model:   "deciding",
		Usage:   provider.TokenUsage{PromptTokens: 30, CompletionTokens: 20, TotalTokens: 50},
	}, nil
}
func (d *decidingLLM) Name() string     { return "deciding" }
func (d *decidingLLM) Models() []string { return []string{"deciding"} }

// countingLineageWriter mirrors stubLineageWriter but lets tests inject a
// non-zero prior conflict count to exercise the diminishing-haircut path.
type countingLineageWriter struct {
	priorCount int
	creates    []model.MemoryLineage
}

func (c *countingLineageWriter) Create(_ context.Context, l *model.MemoryLineage) error {
	c.creates = append(c.creates, *l)
	return nil
}
func (c *countingLineageWriter) CountConflictsBetween(_ context.Context, _, _, _ uuid.UUID) (int, error) {
	return c.priorCount, nil
}

// haircutMemories returns N memories with starting confidence 1.0 and
// staggered IDs so neighbour selection is deterministic. Stamps are absent so
// the contradiction phase treats every row as stale.
func haircutMemories(n int) []model.Memory {
	out := makeMemories(n)
	for i := range out {
		out[i].Confidence = 1.0
	}
	return out
}

func runContradictionCycle(t *testing.T, llm provider.LLMProvider, mems []model.Memory, lineage LineageWriter) *updatingMemoryWriter {
	t.Helper()
	reader := &fakeMemoryReader{list: mems}
	writer := &updatingMemoryWriter{}
	phase := NewContradictionPhase(
		reader,
		writer,
		lineage,
		func() provider.LLMProvider { return llm },
		nilEmbedder,
		stubSettings{},
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)
	budget := NewTokenBudget(1_000_000, 2048)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	return writer
}

// findUpdate returns the most recent recorded Update to a memory by ID, or
// nil if there was none. Tests use this to assert per-memory deltas.
func findUpdate(updates []model.Memory, id uuid.UUID) *model.Memory {
	for i := len(updates) - 1; i >= 0; i-- {
		if updates[i].ID == id {
			cp := updates[i]
			return &cp
		}
	}
	return nil
}

// approxEq verifies two confidence values agree to 4 decimals; accounts for
// the multiplicative noise inherent in the haircut diminishing function.
func approxEq(a, b float64) bool { return math.Abs(a-b) < 1e-4 }

func TestContradictionPhase_LoserHaircut(t *testing.T) {
	llm := &decidingLLM{winner: WinnerSideA}
	mems := haircutMemories(2)
	lineage := &countingLineageWriter{}
	w := runContradictionCycle(t, llm, mems, lineage)

	// pair[0] is statement A → winner gets the small haircut.
	winner := findUpdate(w.updates, mems[0].ID)
	loser := findUpdate(w.updates, mems[1].ID)
	if winner == nil || loser == nil {
		t.Fatalf("expected updates for both pair members; got winner=%v loser=%v", winner, loser)
	}
	if !approxEq(winner.Confidence, 0.97) {
		t.Errorf("winner confidence = %.4f, want 0.97", winner.Confidence)
	}
	if !approxEq(loser.Confidence, 0.85) {
		t.Errorf("loser confidence = %.4f, want 0.85", loser.Confidence)
	}
	if len(lineage.creates) == 0 {
		t.Fatal("expected a conflicts_with edge to be written")
	}
}

func TestContradictionPhase_TieHaircut(t *testing.T) {
	llm := &decidingLLM{winner: WinnerTie}
	mems := haircutMemories(2)
	lineage := &countingLineageWriter{}
	w := runContradictionCycle(t, llm, mems, lineage)

	for _, m := range mems {
		got := findUpdate(w.updates, m.ID)
		if got == nil {
			t.Fatalf("expected update for %s", m.ID)
		}
		if !approxEq(got.Confidence, 0.92) {
			t.Errorf("tie confidence = %.4f, want 0.92", got.Confidence)
		}
	}
}

// TestContradictionPhase_LegacyPromptFallback guards the contract that an
// empty "winner" field (operator running a custom prompt that predates the
// field) normalizes to tie. Permanent test; remove only when no deployment
// can plausibly still be on a pre-winner prompt.
func TestContradictionPhase_LegacyPromptFallback(t *testing.T) {
	llm := &decidingLLM{winner: ""}
	mems := haircutMemories(2)
	lineage := &countingLineageWriter{}
	w := runContradictionCycle(t, llm, mems, lineage)

	// Empty winner normalizes to tie at the call site.
	for _, m := range mems {
		got := findUpdate(w.updates, m.ID)
		if got == nil {
			t.Fatalf("expected update for %s", m.ID)
		}
		if !approxEq(got.Confidence, 0.92) {
			t.Errorf("legacy-fallback confidence = %.4f, want tie 0.92", got.Confidence)
		}
	}
}

func TestContradictionPhase_DiminishingReaffirmation(t *testing.T) {
	llm := &decidingLLM{winner: WinnerSideA}
	mems := haircutMemories(2)
	// Pretend one prior conflict already exists; this is detection #2.
	// Diminished factor: 1 - (1 - base) / 2.
	// loser:  1 - 0.15/2 = 0.925
	// winner: 1 - 0.03/2 = 0.985
	lineage := &countingLineageWriter{priorCount: 1}
	w := runContradictionCycle(t, llm, mems, lineage)

	winner := findUpdate(w.updates, mems[0].ID)
	loser := findUpdate(w.updates, mems[1].ID)
	if winner == nil || loser == nil {
		t.Fatalf("expected updates for both pair members")
	}
	if !approxEq(winner.Confidence, 0.985) {
		t.Errorf("reaffirm winner conf = %.4f, want 0.985", winner.Confidence)
	}
	if !approxEq(loser.Confidence, 0.925) {
		t.Errorf("reaffirm loser conf = %.4f, want 0.925", loser.Confidence)
	}
}

func TestContradictionPhase_HighConfidenceWinnerSupersedes(t *testing.T) {
	llm := &decidingLLM{winner: WinnerSideA}
	mems := haircutMemories(2)
	mems[0].Confidence = 0.95 // pre-haircut > 0.85 supersession threshold
	lineage := &countingLineageWriter{}
	w := runContradictionCycle(t, llm, mems, lineage)

	loser := findUpdate(w.updates, mems[1].ID)
	if loser == nil {
		t.Fatalf("expected update for loser")
	}
	if loser.SupersededBy == nil {
		t.Fatal("expected loser.SupersededBy to be set when winner conf > threshold")
	}
	if *loser.SupersededBy != mems[0].ID {
		t.Errorf("loser.SupersededBy = %s, want winner ID %s", *loser.SupersededBy, mems[0].ID)
	}
	if loser.SupersededAt == nil {
		t.Error("expected loser.SupersededAt to be set alongside SupersededBy")
	}
}

func TestContradictionPhase_LowConfidenceWinnerNoSupersede(t *testing.T) {
	llm := &decidingLLM{winner: WinnerSideA}
	mems := haircutMemories(2)
	mems[0].Confidence = 0.5 // below 0.85 threshold
	lineage := &countingLineageWriter{}
	w := runContradictionCycle(t, llm, mems, lineage)

	loser := findUpdate(w.updates, mems[1].ID)
	if loser == nil {
		t.Fatalf("expected update for loser")
	}
	if loser.SupersededBy != nil {
		t.Errorf("expected SupersededBy nil for low-confidence winner; got %s", *loser.SupersededBy)
	}
}

// --- vector-store-first contradiction phase tests ---

// fakeContradictionVectorStore is a flexible double for the
// vector-store-first selectNeighborPairs path. Tests seed vectorsByID with
// the embeddings they want the phase to read, optionally set getErr /
// searchErr to drive failure paths, and inspect the recorded calls.
type fakeContradictionVectorStore struct {
	vectorsByID map[uuid.UUID][]float32
	getErr      error
	searchErr   error

	getCalls    int
	upsertCalls int
	upsertItems []storage.VectorUpsertItem
	deleteCalls int
	deleted     []uuid.UUID
	searchCalls int
}

func (f *fakeContradictionVectorStore) Upsert(_ context.Context, _ uuid.UUID, _ uuid.UUID, _ []float32, _ int) error {
	return nil
}
func (f *fakeContradictionVectorStore) UpsertBatch(_ context.Context, items []storage.VectorUpsertItem) error {
	f.upsertCalls++
	f.upsertItems = append(f.upsertItems, items...)
	if f.vectorsByID == nil {
		f.vectorsByID = make(map[uuid.UUID][]float32, len(items))
	}
	for _, it := range items {
		cp := make([]float32, len(it.Embedding))
		copy(cp, it.Embedding)
		f.vectorsByID[it.ID] = cp
	}
	return nil
}
func (f *fakeContradictionVectorStore) Search(_ context.Context, _ []float32, _ uuid.UUID, _ int, _ int) ([]storage.VectorSearchResult, error) {
	f.searchCalls++
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	// Tests that exercise the Search path care only that it runs and that
	// the count is observable; downstream candidate selection works equally
	// well from the in-process top-K fallback the empty result triggers.
	return nil, nil
}
func (f *fakeContradictionVectorStore) GetByIDs(_ context.Context, ids []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	f.getCalls++
	if f.getErr != nil {
		return nil, f.getErr
	}
	out := make(map[uuid.UUID][]float32, len(ids))
	for _, id := range ids {
		if v, ok := f.vectorsByID[id]; ok {
			cp := make([]float32, len(v))
			copy(cp, v)
			out[id] = cp
		}
	}
	return out, nil
}
func (f *fakeContradictionVectorStore) Delete(_ context.Context, id uuid.UUID) error {
	f.deleteCalls++
	f.deleted = append(f.deleted, id)
	return nil
}
func (f *fakeContradictionVectorStore) Ping(_ context.Context) error { return nil }

// paraphraseSettings returns a settings stub that turns paraphrase fast-path
// on at the configured threshold and supplies the contradiction prompt
// template the phase fmt.Sprintfs onto pair contents. Threshold defaults
// to 0.97 to mirror the production default.
func paraphraseSettings(enabled bool, threshold float64) *staticDreamSettings {
	values := map[string]string{
		service.SettingDreamContradictionPrompt: "Compare:\nA: %s\nB: %s\nReturn JSON.",
	}
	if enabled {
		values[service.SettingDreamContradictionParaphraseEnabled] = "true"
	}
	floats := map[string]float64{
		service.SettingDreamContradictionParaphraseThreshold: threshold,
		service.SettingDreamContradictionLoserHaircut:        0.85,
		service.SettingDreamContradictionWinnerHaircut:       0.97,
		service.SettingDreamContradictionTieHaircut:          0.92,
		service.SettingDreamSupersessionThreshold:            0.85,
	}
	return &staticDreamSettings{values: values, floats: floats, ints: map[string]int{}}
}

// vectorOffsetAt produces a unit-direction vector that points along axis
// `axis`, used to drive cosine similarity ≈ 0 between memories so the
// paraphrase fast-path does NOT trigger (every off-axis pair is orthogonal).
func vectorOffsetAt(dim, axis int) []float32 {
	v := make([]float32, dim)
	v[axis%dim] = 1.0
	return v
}

func TestContradictionPhase_VectorStoreHitsAvoidEmbedding(t *testing.T) {
	mems := makeMemories(4)
	dim := 4
	vs := &fakeContradictionVectorStore{vectorsByID: map[uuid.UUID][]float32{}}
	for i := range mems {
		vs.vectorsByID[mems[i].ID] = vectorOffsetAt(dim, i)
	}
	emb := &staticEmbedder{vectors: [][]float32{vectorOffsetAt(dim, 0)}}
	llm := &zeroUsageLLM{}

	phase := NewContradictionPhase(
		&fakeMemoryReader{list: mems},
		&updatingMemoryWriter{},
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		paraphraseSettings(false, 0.97), // disabled — exercise only the read path
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)
	phase.AttachVectorStore(vs)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1_000_000, 2048),
		NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if emb.calls.Load() != 0 {
		t.Errorf("expected zero embedder calls when every vector is in the store, got %d", emb.calls.Load())
	}
	if vs.getCalls == 0 {
		t.Error("expected at least one GetByIDs call")
	}
	if vs.upsertCalls != 0 {
		t.Errorf("expected zero self-heal upserts when no misses, got %d", vs.upsertCalls)
	}
}

func TestContradictionPhase_VectorStoreMissesTriggerEmbed(t *testing.T) {
	mems := makeMemories(4)
	dim := 4
	vs := &fakeContradictionVectorStore{vectorsByID: map[uuid.UUID][]float32{}}
	// Half the memories are stored, half are misses.
	for i := 0; i < 2; i++ {
		vs.vectorsByID[mems[i].ID] = vectorOffsetAt(dim, i)
	}
	emb := &staticEmbedder{vectors: [][]float32{
		vectorOffsetAt(dim, 2),
		vectorOffsetAt(dim, 3),
	}}
	llm := &zeroUsageLLM{}

	phase := NewContradictionPhase(
		&fakeMemoryReader{list: mems},
		&updatingMemoryWriter{},
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		paraphraseSettings(false, 0.97),
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)
	phase.AttachVectorStore(vs)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1_000_000, 2048),
		NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if emb.calls.Load() != 1 {
		t.Errorf("expected exactly 1 embedder call (one batched miss-set), got %d", emb.calls.Load())
	}
	if vs.upsertCalls == 0 {
		t.Error("expected self-heal UpsertBatch to be called for the miss set")
	}
	if len(vs.upsertItems) != 2 {
		t.Errorf("expected 2 upsert items (one per miss), got %d", len(vs.upsertItems))
	}
}

func TestContradictionPhase_VectorStoreErrorFallsBackToFullEmbed(t *testing.T) {
	mems := makeMemories(4)
	dim := 4
	vs := &fakeContradictionVectorStore{
		vectorsByID: map[uuid.UUID][]float32{},
		getErr:      errors.New("vector-store offline"),
	}
	emb := &staticEmbedder{vectors: [][]float32{
		vectorOffsetAt(dim, 0),
		vectorOffsetAt(dim, 1),
		vectorOffsetAt(dim, 2),
		vectorOffsetAt(dim, 3),
	}}
	llm := &zeroUsageLLM{}

	phase := NewContradictionPhase(
		&fakeMemoryReader{list: mems},
		&updatingMemoryWriter{},
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		paraphraseSettings(false, 0.97),
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)
	phase.AttachVectorStore(vs)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1_000_000, 2048),
		NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if emb.calls.Load() == 0 {
		t.Error("expected embedder to be called when GetByIDs errors (full re-embed fallback)")
	}
	if llm.calls.Load() == 0 {
		t.Error("expected LLM calls — phase must still emit pairs after store failure")
	}
}

func TestContradictionPhase_ParaphraseAutoSupersede(t *testing.T) {
	mems := makeMemories(2)
	mems[0].Confidence = 0.9
	mems[1].Confidence = 0.5
	dim := 4
	dup := vectorOffsetAt(dim, 0) // both vectors identical → cosine = 1.0
	vs := &fakeContradictionVectorStore{
		vectorsByID: map[uuid.UUID][]float32{
			mems[0].ID: dup,
			mems[1].ID: dup,
		},
	}
	emb := &staticEmbedder{vectors: [][]float32{dup}}
	llm := &decidingLLM{winner: WinnerSideA} // would also be reached if fast-path skipped

	writer := &updatingMemoryWriter{}
	phase := NewContradictionPhase(
		&fakeMemoryReader{list: mems},
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		paraphraseSettings(true, 0.97),
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)
	phase.AttachVectorStore(vs)
	phase.AttachVectorPurger(vs)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1_000_000, 2048),
		NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if llm.calls.Load() != 0 {
		t.Errorf("expected LLM judge skipped on paraphrase fast-path, got %d calls", llm.calls.Load())
	}

	// The lower-confidence side (mems[1]) must be the loser.
	loser := findUpdate(writer.updates, mems[1].ID)
	if loser == nil {
		t.Fatalf("expected update for loser memory %s", mems[1].ID)
	}
	if loser.SupersededBy == nil {
		t.Fatal("expected loser.SupersededBy to be set")
	}
	if *loser.SupersededBy != mems[0].ID {
		t.Errorf("loser.SupersededBy = %s, want winner %s", *loser.SupersededBy, mems[0].ID)
	}
	if loser.SupersededAt == nil {
		t.Error("expected loser.SupersededAt to be set")
	}

	if vs.deleteCalls == 0 || vs.deleted[0] != mems[1].ID {
		t.Errorf("expected vector purger Delete on loser %s, got deletes=%v", mems[1].ID, vs.deleted)
	}
}

func TestContradictionPhase_ParaphraseDisabledKeepsLLMPath(t *testing.T) {
	mems := makeMemories(2)
	mems[0].Confidence = 0.9
	mems[1].Confidence = 0.5
	dim := 4
	dup := vectorOffsetAt(dim, 0)
	vs := &fakeContradictionVectorStore{
		vectorsByID: map[uuid.UUID][]float32{
			mems[0].ID: dup,
			mems[1].ID: dup,
		},
	}
	emb := &staticEmbedder{vectors: [][]float32{dup}}
	llm := &decidingLLM{winner: WinnerSideA}

	phase := NewContradictionPhase(
		&fakeMemoryReader{list: mems},
		&updatingMemoryWriter{},
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		paraphraseSettings(false, 0.97),
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)
	phase.AttachVectorStore(vs)
	phase.AttachVectorPurger(vs)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1_000_000, 2048),
		NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if llm.calls.Load() == 0 {
		t.Error("expected LLM judge to run when paraphrase fast-path is disabled")
	}
}

func TestContradictionPhase_ParaphraseTiebreakOlderWins(t *testing.T) {
	mems := makeMemories(2)
	// Equal confidence; older CreatedAt should survive.
	mems[0].Confidence = 0.7
	mems[1].Confidence = 0.7
	older := time.Now().UTC().Add(-2 * time.Hour)
	newer := time.Now().UTC().Add(-1 * time.Hour)
	mems[0].CreatedAt = newer
	mems[1].CreatedAt = older
	dim := 4
	dup := vectorOffsetAt(dim, 0)
	vs := &fakeContradictionVectorStore{
		vectorsByID: map[uuid.UUID][]float32{
			mems[0].ID: dup,
			mems[1].ID: dup,
		},
	}
	emb := &staticEmbedder{vectors: [][]float32{dup}}

	writer := &updatingMemoryWriter{}
	phase := NewContradictionPhase(
		&fakeMemoryReader{list: mems},
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return &zeroUsageLLM{} },
		func() provider.EmbeddingProvider { return emb },
		paraphraseSettings(true, 0.97),
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)
	phase.AttachVectorStore(vs)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1_000_000, 2048),
		NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// mems[1] is older → winner; mems[0] (newer) is loser.
	loser := findUpdate(writer.updates, mems[0].ID)
	if loser == nil {
		t.Fatalf("expected update for loser %s (newer CreatedAt)", mems[0].ID)
	}
	if loser.SupersededBy == nil || *loser.SupersededBy != mems[1].ID {
		got := "<nil>"
		if loser.SupersededBy != nil {
			got = loser.SupersededBy.String()
		}
		t.Errorf("expected loser.SupersededBy=%s (older), got %s", mems[1].ID, got)
	}
}

func TestContradictionPhase_SearchFailureUsesInProcessTopK(t *testing.T) {
	mems := makeMemories(4)
	dim := 4
	vs := &fakeContradictionVectorStore{
		vectorsByID: map[uuid.UUID][]float32{},
		searchErr:   errors.New("index unavailable"),
	}
	for i := range mems {
		vs.vectorsByID[mems[i].ID] = vectorOffsetAt(dim, i)
	}
	emb := &staticEmbedder{vectors: [][]float32{vectorOffsetAt(dim, 0)}}
	llm := &zeroUsageLLM{}

	phase := NewContradictionPhase(
		&fakeMemoryReader{list: mems},
		&updatingMemoryWriter{},
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		paraphraseSettings(false, 0.97),
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
	)
	phase.AttachVectorStore(vs)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: mems[0].NamespaceID}
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1_000_000, 2048),
		NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if vs.searchCalls == 0 {
		t.Error("expected at least one Search attempt before fallback")
	}
	if llm.calls.Load() == 0 {
		t.Error("expected pairs to still be dispatched via in-process top-K fallback")
	}
}
