package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/provider"
)

// fakeReranker is a deterministic RerankProvider for the stage tests. It returns
// a fixed score slice (aligned to input docs) and counts calls so a test can
// assert the stage was skipped entirely.
type fakeReranker struct {
	scores []float64
	err    error
	calls  int
}

func (f *fakeReranker) Name() string { return "fake-reranker" }

func (f *fakeReranker) Rerank(_ context.Context, _ string, docs []string) (*provider.RerankResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	// Default to one score per doc when the test did not preset a slice, so the
	// length-match guard in the stage passes.
	scores := f.scores
	if scores == nil {
		scores = make([]float64, len(docs))
	}
	return &provider.RerankResponse{Scores: scores, Model: "fake"}, nil
}

func rerankTestSettings(t *testing.T, enabledKey string, on bool) *SettingsService {
	t.Helper()
	repo := newMockSettingsRepo()
	if on {
		repo.put(enabledKey, "global", "true")
	} else {
		repo.put(enabledKey, "global", "false")
	}
	return NewSettingsService(repo)
}

// --- recall stage ----------------------------------------------------------

func TestRerankRecall_DisabledIsNoop(t *testing.T) {
	fake := &fakeReranker{scores: []float64{0, 1}}
	s := &RecallService{
		settings:       rerankTestSettings(t, SettingRerankEnabled, false),
		rerankProvider: func() provider.RerankProvider { return fake },
	}
	in := []RecallResult{
		{Content: "A", Score: 1.0},
		{Content: "B", Score: 0.5},
	}
	out := s.rerankRecall(context.Background(), "q", in, 10)

	if fake.calls != 0 {
		t.Errorf("reranker must not be called when disabled, got %d calls", fake.calls)
	}
	if out[0].Content != "A" || out[1].Content != "B" {
		t.Errorf("disabled stage must keep composite order, got %q,%q", out[0].Content, out[1].Content)
	}
	if out[0].RerankScore != nil || out[1].RerankScore != nil {
		t.Error("disabled stage must not stamp RerankScore")
	}
}

func TestRerankRecall_EnabledReordersAndFoldsIn(t *testing.T) {
	// A leads on composite (1.0 vs 0.5); the reranker scores B higher (0,1).
	// With lambda 10, B's folded score (0.5+10) overtakes A's (1.0+0).
	fake := &fakeReranker{scores: []float64{0, 1}}
	s := &RecallService{
		settings:       rerankTestSettings(t, SettingRerankEnabled, true),
		rerankProvider: func() provider.RerankProvider { return fake },
	}
	in := []RecallResult{
		{Content: "A", Score: 1.0},
		{Content: "B", Score: 0.5},
	}
	out := s.rerankRecall(context.Background(), "q", in, 10)

	if fake.calls != 1 {
		t.Fatalf("reranker should be called once, got %d", fake.calls)
	}
	if out[0].Content != "B" || out[1].Content != "A" {
		t.Fatalf("enabled stage must reorder by folded score, got %q,%q", out[0].Content, out[1].Content)
	}
	// Audit field stamped and additive fold-in applied: B = 0.5 + 10*1 = 10.5.
	if out[0].RerankScore == nil || *out[0].RerankScore != 1 {
		t.Errorf("B RerankScore = %v, want 1", out[0].RerankScore)
	}
	if out[0].Score != 10.5 {
		t.Errorf("B folded score = %v, want 10.5", out[0].Score)
	}
	if out[1].Score != 1.0 { // A = 1.0 + 10*0
		t.Errorf("A folded score = %v, want 1.0", out[1].Score)
	}
}

func TestRerankRecall_ZeroLambdaIsNoop(t *testing.T) {
	fake := &fakeReranker{scores: []float64{0, 1}}
	s := &RecallService{
		settings:       rerankTestSettings(t, SettingRerankEnabled, true),
		rerankProvider: func() provider.RerankProvider { return fake },
	}
	in := []RecallResult{{Content: "A", Score: 1.0}, {Content: "B", Score: 0.5}}
	out := s.rerankRecall(context.Background(), "q", in, 0)
	if fake.calls != 0 {
		t.Errorf("zero lambda must short-circuit before calling the reranker, got %d calls", fake.calls)
	}
	if out[0].Content != "A" {
		t.Error("zero lambda must keep composite order")
	}
}

func TestRerankRecall_FailSoftOnError(t *testing.T) {
	fake := &fakeReranker{err: errors.New("upstream down")}
	s := &RecallService{
		settings:       rerankTestSettings(t, SettingRerankEnabled, true),
		rerankProvider: func() provider.RerankProvider { return fake },
	}
	in := []RecallResult{{Content: "A", Score: 1.0}, {Content: "B", Score: 0.5}}
	out := s.rerankRecall(context.Background(), "q", in, 10)
	if out[0].Content != "A" || out[1].Content != "B" {
		t.Error("a reranker error must leave the composite order intact (fail-soft)")
	}
}

func TestRerankRecall_NoProviderIsNoop(t *testing.T) {
	s := &RecallService{settings: rerankTestSettings(t, SettingRerankEnabled, true)}
	in := []RecallResult{{Content: "A", Score: 1.0}, {Content: "B", Score: 0.5}}
	out := s.rerankRecall(context.Background(), "q", in, 10)
	if out[0].Content != "A" {
		t.Error("no wired reranker must leave order unchanged")
	}
}

// --- ask stage -------------------------------------------------------------

func TestRerankNeighborhood_DisabledIsNoop(t *testing.T) {
	fake := &fakeReranker{scores: []float64{0, 1}}
	s := &AskService{
		settings: rerankTestSettings(t, SettingAskRerankEnabled, false),
		reranker: func() provider.RerankProvider { return fake },
	}
	in := []neighborMemory{{shortID: "a", content: "A"}, {shortID: "b", content: "B"}}
	out := s.rerankNeighborhood(context.Background(), "q", in)
	if fake.calls != 0 {
		t.Errorf("reranker must not be called when disabled, got %d calls", fake.calls)
	}
	if out[0].shortID != "a" || out[1].shortID != "b" {
		t.Errorf("disabled stage must keep assembly order, got %q,%q", out[0].shortID, out[1].shortID)
	}
}

func TestRerankNeighborhood_EnabledReorders(t *testing.T) {
	// Reranker scores the second member higher, so it should lead after rerank.
	fake := &fakeReranker{scores: []float64{0.1, 0.9}}
	s := &AskService{
		settings: rerankTestSettings(t, SettingAskRerankEnabled, true),
		reranker: func() provider.RerankProvider { return fake },
	}
	in := []neighborMemory{{shortID: "a", content: "A"}, {shortID: "b", content: "B"}}
	out := s.rerankNeighborhood(context.Background(), "q", in)
	if fake.calls != 1 {
		t.Fatalf("reranker should be called once, got %d", fake.calls)
	}
	if out[0].shortID != "b" || out[1].shortID != "a" {
		t.Fatalf("enabled stage must reorder by score desc, got %q,%q", out[0].shortID, out[1].shortID)
	}
}

func TestTruncateForRerank(t *testing.T) {
	const cap = 1200
	// Short strings (and a non-positive cap) pass through untouched.
	if got := truncateForRerank("hello", cap); got != "hello" {
		t.Errorf("short string altered: %q", got)
	}
	if got := truncateForRerank("anything", 0); got != "anything" {
		t.Errorf("zero cap must pass through, got %q", got)
	}
	// Long ASCII is cut to the cap.
	long := strings.Repeat("a", cap+500)
	if got := truncateForRerank(long, cap); len(got) != cap {
		t.Errorf("len after truncate = %d, want %d", len(got), cap)
	}
	// A multi-byte rune straddling the cap must not be split (result stays valid UTF-8).
	mb := strings.Repeat("a", cap-1) + "€€€" // '€' is 3 bytes
	got := truncateForRerank(mb, cap)
	if len(got) > cap {
		t.Errorf("len after truncate = %d, exceeds cap %d", len(got), cap)
	}
	if !utf8.ValidString(got) {
		t.Errorf("truncation split a rune: %q is not valid UTF-8", got)
	}
}

func TestRerankNeighborhood_FailSoftOnError(t *testing.T) {
	fake := &fakeReranker{err: errors.New("down")}
	s := &AskService{
		settings: rerankTestSettings(t, SettingAskRerankEnabled, true),
		reranker: func() provider.RerankProvider { return fake },
	}
	in := []neighborMemory{{shortID: "a", content: "A"}, {shortID: "b", content: "B"}}
	out := s.rerankNeighborhood(context.Background(), "q", in)
	if out[0].shortID != "a" || out[1].shortID != "b" {
		t.Error("a reranker error must leave the neighborhood order intact (fail-soft)")
	}
}

// TestRerankRecall_WindowBoundsCandidates verifies ranking.rerank.candidates caps
// how many candidates are scored: only the top-window get a RerankScore, the tail
// keeps its composite position untouched.
func TestRerankRecall_WindowBoundsCandidates(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingRerankEnabled, "global", "true")
	repo.put(SettingRerankCandidates, "global", "2")
	s := &RecallService{
		settings:       NewSettingsService(repo),
		rerankProvider: func() provider.RerankProvider { return &fakeReranker{scores: []float64{0.1, 0.2}} },
	}
	in := []RecallResult{
		{Content: "A", Score: 1.0}, {Content: "B", Score: 0.9}, {Content: "C", Score: 0.8},
		{Content: "D", Score: 0.7}, {Content: "E", Score: 0.6},
	}
	out := s.rerankRecall(context.Background(), "q", in, 1)
	scored := 0
	for _, m := range out {
		if m.RerankScore != nil {
			scored++
		}
	}
	if scored != 2 {
		t.Fatalf("window=2 must score exactly 2 candidates, got %d", scored)
	}
}

// TestMMRNeighborhood_DemotesDuplicate proves the always-on ask MMR pass reorders
// the neighborhood: an exact duplicate of the top member is demoted below a more
// diverse member. Uses a diversity-heavy lambda (0.3) so the redundancy penalty
// clearly bites, and a fake hydrator supplying the embeddings.
func TestMMRNeighborhood_DemotesDuplicate(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingRankWeightMmr, "global", "0.3")
	aID, bID, cID := uuid.New(), uuid.New(), uuid.New()
	vec := askFakeVectors{embs: map[uuid.UUID][]float32{
		aID: {1, 0, 0}, bID: {1, 0, 0}, cID: {0.7, 0.7, 0}, // B is an exact dup of A; C is diverse
	}}
	s := &AskService{settings: NewSettingsService(repo), vectors: vec}
	nb := []neighborMemory{
		{memoryID: aID, content: "A"}, {memoryID: bID, content: "B"}, {memoryID: cID, content: "C"},
	}
	out := s.mmrNeighborhood(context.Background(), nb, []float32{1, 0, 0}, 3)
	if len(out) != 3 {
		t.Fatalf("MMR must preserve all members, got %d", len(out))
	}
	got := []string{out[0].content, out[1].content, out[2].content}
	if got[0] != "A" || got[1] != "C" || got[2] != "B" {
		t.Fatalf("MMR should keep A first and demote duplicate B below diverse C, got %v", got)
	}
}
