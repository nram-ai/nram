package service

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// countingLLM is a decomposition-provider double: it returns a fixed reply and
// counts Complete calls so a test can assert whether (and how often) the
// decomposer LLM was invoked.
type countingLLM struct {
	reply string
	mu    sync.Mutex
	calls int
}

func (f *countingLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return &provider.CompletionResponse{Content: f.reply}, nil
}
func (*countingLLM) Name() string     { return "decomp" }
func (*countingLLM) Models() []string { return []string{"decomp"} }
func (f *countingLLM) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// --- pure-function unit tests ---

func TestHasMultiFacetSignal(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  bool
	}{
		{"no comma no conjunction", "brandon health conditions", false},
		{"keyword bag no conjunction", "recall retrieval reserved tiers global about me corpus skew", false},
		{"enumerating conjunction", "brandon health finances ebay and hardware preferences", true},
		{"commas", "my health, my finances, my ebay listings, my hardware", true},
		{"conjunction present (length is shouldDecompose's job)", "cats and dogs", true},
		{"vs / versus / compare", "compare my velocity project versus my nram project", true},
		{"only long words, none are keywords", "understanding relationships between distinct concepts thoroughly", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasMultiFacetSignal(tc.query); got != tc.want {
				t.Errorf("hasMultiFacetSignal(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestInterleaveRecall(t *testing.T) {
	id := func(n byte) uuid.UUID { return uuid.UUID{n} }
	res := func(n byte, score float64) RecallResult { return RecallResult{ID: id(n), Score: score} }

	// Response A (original): high scores. Response B (a sub-query): its own top is
	// 0.4, so with floorRatio 0.5 only >= 0.2 survive.
	respA := &RecallResponse{Memories: []RecallResult{res(1, 0.9), res(2, 0.8)}}
	respB := &RecallResponse{Memories: []RecallResult{res(3, 0.4), res(1, 0.35), res(4, 0.1)}}

	got := interleaveRecall([]*RecallResponse{respA, respB, nil}, 0.5, 5)

	// Round-robin: A[0]=1, B[0]=3, A[1]=2, B[1]=1(dup, skipped), then B[2]=0.1 is
	// below the 0.2 floor so it never entered. Result: [1,3,2].
	wantOrder := []uuid.UUID{id(1), id(3), id(2)}
	if len(got) != len(wantOrder) {
		t.Fatalf("got %d results, want %d: %+v", len(got), len(wantOrder), got)
	}
	for i, w := range wantOrder {
		if got[i].ID != w {
			t.Errorf("position %d = %v, want %v", i, got[i].ID, w)
		}
	}
}

func TestInterleaveRecall_TruncatesToLimit(t *testing.T) {
	mk := func(n byte) RecallResult { return RecallResult{ID: uuid.UUID{n}, Score: 0.5} }
	resp := &RecallResponse{Memories: []RecallResult{mk(1), mk(2), mk(3), mk(4)}}
	got := interleaveRecall([]*RecallResponse{resp}, 0, 2)
	if len(got) != 2 {
		t.Fatalf("expected truncation to 2, got %d", len(got))
	}
}

// --- orchestration / gating tests ---

// newDecomposeTestSvc builds a list-fallback recall service (no embedder/vector
// store, so recallSingle returns the namespace memories for any query) wired
// with the real settings defaults and a scripted decomposition LLM.
func newDecomposeTestSvc(t *testing.T, reply string) (*RecallService, uuid.UUID, *countingLLM) {
	t.Helper()
	projectID, nsID, projects, namespaces := setupTestFixtures()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{},
		nsList: []model.Memory{
			*makeTestMemory(uuid.New(), nsID, "a memory", []string{"t"}, 0.5, 0, time.Now()),
			*makeTestMemory(uuid.New(), nsID, "another memory", []string{"t"}, 0.5, 0, time.Now()),
		},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)
	svc.SetSettings(NewSettingsService(newMockSettingsRepo()))
	llm := &countingLLM{reply: reply}
	svc.SetDecomposer(func() provider.LLMProvider { return llm })
	return svc, projectID, llm
}

const multiFacetQuery = "my health, my finances, my ebay listings, and my hardware preferences"

func TestRecall_Decomposition_SplitsMultiFacetQuery(t *testing.T) {
	svc, projectID, llm := newDecomposeTestSvc(t, `{"subqueries":["health","finances","ebay","hardware"]}`)

	resp, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: multiFacetQuery, Limit: 10})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if llm.callCount() != 1 {
		t.Errorf("decomposer LLM should be called exactly once, got %d", llm.callCount())
	}
	if resp.SubqueryCount != 4 {
		t.Errorf("SubqueryCount = %d, want 4", resp.SubqueryCount)
	}
	if len(resp.Memories) == 0 {
		t.Error("decomposed recall returned no memories")
	}
}

func TestRecall_Decomposition_EmptyDecompositionFallsThrough(t *testing.T) {
	svc, projectID, llm := newDecomposeTestSvc(t, `{"subqueries":[]}`)

	resp, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: multiFacetQuery, Limit: 10})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if llm.callCount() != 1 {
		t.Errorf("decomposer LLM should be consulted once, got %d", llm.callCount())
	}
	if resp.SubqueryCount != 0 {
		t.Errorf("empty decomposition must leave SubqueryCount 0, got %d", resp.SubqueryCount)
	}
}

func TestRecall_Decomposition_PreFilterSkipsLLM(t *testing.T) {
	// Two ways the pre-filter rejects before the LLM: no multi-facet signal, and
	// a signal present but the query below the min-words length gate.
	for _, q := range []string{"hello world", "cats and dogs"} {
		svc, projectID, llm := newDecomposeTestSvc(t, `{"subqueries":["a","b"]}`)
		if _, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: q, Limit: 10}); err != nil {
			t.Fatalf("recall %q: %v", q, err)
		}
		if llm.callCount() != 0 {
			t.Errorf("query %q must skip the decomposer LLM (pre-filter), got %d calls", q, llm.callCount())
		}
	}
}

func TestRecall_Decomposition_PerCallOverrideDisables(t *testing.T) {
	svc, projectID, llm := newDecomposeTestSvc(t, `{"subqueries":["a","b"]}`)
	off := false

	if _, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: multiFacetQuery, Limit: 10, Decompose: &off}); err != nil {
		t.Fatalf("recall: %v", err)
	}
	if llm.callCount() != 0 {
		t.Errorf("Decompose=false must skip the decomposer LLM, got %d calls", llm.callCount())
	}
}

func TestRecall_Decomposition_DiversifyIsMutuallyExclusive(t *testing.T) {
	svc, projectID, llm := newDecomposeTestSvc(t, `{"subqueries":["a","b"]}`)

	_, err := svc.Recall(context.Background(), &RecallRequest{
		ProjectID:            projectID,
		Query:                multiFacetQuery,
		Limit:                10,
		DiversifyByTagPrefix: "category-",
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if llm.callCount() != 0 {
		t.Errorf("DiversifyByTagPrefix must skip decomposition, got %d decomposer calls", llm.callCount())
	}
}

func TestRecall_Decomposition_NoDecomposerWiredIsSingleVector(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	memReader := &mockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{},
		nsList:   []model.Memory{*makeTestMemory(uuid.New(), nsID, "m", []string{"t"}, 0.5, 0, time.Now())},
	}
	svc, _ := newRecallService(memReader, projects, namespaces, nil, nil, nil, nil)
	svc.SetSettings(NewSettingsService(newMockSettingsRepo()))
	// No SetDecomposer: decomposition is impossible regardless of the setting.

	resp, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: multiFacetQuery, Limit: 10})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if resp.SubqueryCount != 0 {
		t.Errorf("no decomposer wired must leave SubqueryCount 0, got %d", resp.SubqueryCount)
	}
}

// markerEmbedder returns a 4-dim one-hot embedding whose set index is chosen by a
// marker substring in the query, so a query-aware vector searcher can hand back a
// distinct memory per recall leg (original vs each sub-query).
type markerEmbedder struct{}

func (markerEmbedder) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	idx := 0
	q := strings.ToLower(req.Input[0])
	switch {
	case strings.Contains(q, "healthmarker"):
		idx = 1
	case strings.Contains(q, "financemarker"):
		idx = 2
	}
	emb := make([]float32, 4)
	emb[idx] = 1
	return &provider.EmbeddingResponse{
		Embeddings: [][]float32{emb},
		Model:      "marker",
		Usage:      provider.TokenUsage{PromptTokens: 1, TotalTokens: 1},
	}, nil
}
func (markerEmbedder) Name() string      { return "marker" }
func (markerEmbedder) Dimensions() []int { return []int{4} }

// markerSearcher returns the memory registered for the one-hot index of the query
// embedding, modelling per-facet clusters that only a focused sub-query reaches.
type markerSearcher struct {
	byIdx map[int]storage.VectorSearchResult
}

func (s markerSearcher) Search(_ context.Context, kind storage.VectorKind, emb []float32, ns uuid.UUID, _ int, _ int) ([]storage.VectorSearchResult, error) {
	if kind != storage.VectorKindMemory {
		return nil, nil
	}
	idx := 0
	for i, v := range emb {
		if v > 0 {
			idx = i
			break
		}
	}
	if r, ok := s.byIdx[idx]; ok && r.NamespaceID == ns {
		return []storage.VectorSearchResult{r}, nil
	}
	return nil, nil
}

// TestRecall_Decomposition_InterleavesDistinctSubqueryResults is the end-to-end
// seam proof: the original query's single-vector recall surfaces only its own
// memory, but a decomposed recall interleaves in the facet memories that ONLY the
// focused sub-queries reach — the multi-facet facet-drop this fix targets.
func TestRecall_Decomposition_InterleavesDistinctSubqueryResults(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()
	now := time.Now()
	orig, health, finance := uuid.New(), uuid.New(), uuid.New()
	memReader := &mockMemoryReader{memories: map[uuid.UUID]*model.Memory{
		orig:    makeTestMemory(orig, nsID, "original-hit", []string{"t"}, 0.5, 0, now),
		health:  makeTestMemory(health, nsID, "health-hit", []string{"t"}, 0.5, 0, now),
		finance: makeTestMemory(finance, nsID, "finance-hit", []string{"t"}, 0.5, 0, now),
	}}
	searcher := markerSearcher{byIdx: map[int]storage.VectorSearchResult{
		0: {ID: orig, Score: 0.9, NamespaceID: nsID},
		1: {ID: health, Score: 0.9, NamespaceID: nsID},
		2: {ID: finance, Score: 0.9, NamespaceID: nsID},
	}}
	svc, _ := newRecallService(memReader, projects, namespaces, searcher, nil, nil, func() provider.EmbeddingProvider { return markerEmbedder{} })
	svc.SetSettings(NewSettingsService(newMockSettingsRepo()))
	llm := &countingLLM{reply: `{"subqueries":["healthmarker facet detail","financemarker facet detail"]}`}
	svc.SetDecomposer(func() provider.LLMProvider { return llm })

	// The original query carries no marker (embeds to index 0 -> only the original
	// memory) and is phrased to pass the multi-facet pre-filter.
	q := "give me my first, second, and third things please"

	ids := func(r *RecallResponse) map[uuid.UUID]bool {
		m := map[uuid.UUID]bool{}
		for _, x := range r.Memories {
			m[x.ID] = true
		}
		return m
	}

	// Single-vector baseline (decomposition off) surfaces only the original memory.
	off := false
	base, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: q, Limit: 10, Decompose: &off})
	if err != nil {
		t.Fatalf("baseline recall: %v", err)
	}
	if b := ids(base); !b[orig] || b[health] || b[finance] {
		t.Fatalf("single-vector baseline should surface only the original memory, got %v", b)
	}

	// Decomposed recall interleaves in the two facet memories the sub-queries reach.
	dec, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: q, Limit: 10})
	if err != nil {
		t.Fatalf("decomposed recall: %v", err)
	}
	if dec.SubqueryCount != 2 {
		t.Errorf("SubqueryCount = %d, want 2", dec.SubqueryCount)
	}
	got := ids(dec)
	if !got[orig] || !got[health] || !got[finance] {
		t.Errorf("decomposed recall must interleave original+health+finance, got %v", got)
	}
}

// TestRecall_Decomposition_ReinforcesOnce guards the correctness fix: a
// decomposition fan-out reinforces once over the final interleaved result, not
// once per sub-query leg.
func TestRecall_Decomposition_ReinforcesOnce(t *testing.T) {
	svc, projectID, _ := newDecomposeTestSvc(t, `{"subqueries":["health","finances","ebay","hardware"]}`)

	writer := &recordingReinforcer{}
	svc.SetReinforcement(&ReinforcementDeps{
		Writer:   writer,
		Settings: &staticSettings{values: map[string]string{SettingReconsolidationMode: ReconsolidationModePersist}},
	})

	if _, err := svc.Recall(context.Background(), &RecallRequest{ProjectID: projectID, Query: multiFacetQuery, Limit: 10}); err != nil {
		t.Fatalf("recall: %v", err)
	}

	waitForCalls(t, writer.callCount, 1)
	// Let any erroneous extra sub-query reinforcement goroutines run, then assert
	// exactly one fired (five without the fix: original + four sub-queries).
	time.Sleep(50 * time.Millisecond)
	if n := writer.callCount(); n != 1 {
		t.Errorf("decomposed recall must reinforce once over the final set, got %d calls", n)
	}
}
