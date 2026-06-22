package service

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// askSeqLLM returns a scripted reply per Complete call (call 0 = decomposition,
// call 1 = synthesis) and records each call's user message so a test can assert
// what the synthesizer actually received.
type askSeqLLM struct {
	replies []string
	users   []string
	calls   int
}

func (f *askSeqLLM) Complete(_ context.Context, req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	user := ""
	if len(req.Messages) > 0 {
		user = req.Messages[len(req.Messages)-1].Content
	}
	f.users = append(f.users, user)
	r := ""
	if f.calls < len(f.replies) {
		r = f.replies[f.calls]
	}
	f.calls++
	return &provider.CompletionResponse{Content: r}, nil
}
func (*askSeqLLM) Name() string     { return "seq" }
func (*askSeqLLM) Models() []string { return []string{"seq"} }

// askMapRecaller returns a distinct RecallResponse per request query, modelling
// the per-class clusters a focused sub-query retrieves.
type askMapRecaller struct {
	byQuery map[string]*RecallResponse
	def     *RecallResponse
	queries []string
}

func (f *askMapRecaller) Recall(_ context.Context, req *RecallRequest) (*RecallResponse, error) {
	f.queries = append(f.queries, req.Query)
	if r, ok := f.byQuery[req.Query]; ok {
		return r, nil
	}
	return f.def, nil
}

func TestParseDecomposition(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"envelope", `{"subqueries":["a","b"]}`, []string{"a", "b"}},
		{"bare array", `["a","b"]`, []string{"a", "b"}},
		{"fenced envelope", "```json\n{\"subqueries\":[\"a\"]}\n```", []string{"a"}},
		{"prose around envelope", `Sure: {"subqueries":["x"]} done`, []string{"x"}},
		{"empty list", `{"subqueries":[]}`, nil},
		{"whitespace dropped", `{"subqueries":["a","  "]}`, []string{"a"}},
		{"prose only", `no decomposition needed`, nil},
		{"garbage", `}{[`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseDecomposition(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("index %d: got %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestAsk_DecompositionUnionsClasses is the core fix: a focused C++ sub-query's
// candidates carry lower absolute scores than the majority TypeScript query, yet
// they must reach the synthesis neighborhood because each response is floored
// against its OWN top, not a shared global top. The C++ scores (0.4, 0.3) sit
// below the global floor (0.5*0.9=0.45) but above their own (0.5*0.4=0.2).
func TestAsk_DecompositionUnionsClasses(t *testing.T) {
	rc := &askMapRecaller{
		byQuery: map[string]*RecallResponse{
			"which projects are C++ vs TypeScript": {Memories: []RecallResult{
				askCandidate("aaaaaaaa", "ts", 0.9),
				askCandidate("bbbbbbbb", "ts", 0.85),
			}},
			"projects written in C++": {Memories: []RecallResult{
				askCandidate("cccccccc", "ranshaw", 0.4),
				askCandidate("dddddddd", "ranshaw", 0.3),
			}},
			"projects written in TypeScript": {Memories: []RecallResult{
				askCandidate("eeeeeeee", "ts", 0.88),
			}},
		},
		def: &RecallResponse{},
	}
	llm := &askSeqLLM{replies: []string{
		`{"subqueries":["projects written in C++","projects written in TypeScript"]}`,
		"ts uses TypeScript [aaaaaaaa]; ranshaw is C++ [cccccccc].",
	}}
	svc := newAskSvc(t, &askFakeRecaller{}, &askFakeMem{}, askTestProjects(), llm, nil)
	// newAskSvc wires askFakeRecaller; swap in the map recaller via the service field.
	svc.recall = rc

	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "which projects are C++ vs TypeScript", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatalf("ask: %v", err)
	}
	if resp.SynthesisMeta.SubqueryCount != 2 {
		t.Errorf("expected 2 sub-queries, got %d", resp.SynthesisMeta.SubqueryCount)
	}
	if len(rc.queries) != 3 {
		t.Errorf("expected 3 recalls (original + 2 sub-queries), got %d: %v", len(rc.queries), rc.queries)
	}
	// The synthesizer's user message carries the assembled neighborhood. Both
	// classes must be present — the low-scoring C++ memory most of all, since a
	// global floor would have dropped it.
	if len(llm.users) < 2 {
		t.Fatalf("expected a decomposition call and a synthesis call, got %d", len(llm.users))
	}
	synthUser := llm.users[len(llm.users)-1]
	if !strings.Contains(synthUser, "memory content for cccccccc") {
		t.Errorf("C++ memory (below the global floor) must reach synthesis via its own per-query floor; neighborhood=%q", synthUser)
	}
	if !strings.Contains(synthUser, "memory content for aaaaaaaa") {
		t.Errorf("TypeScript memory must be in the neighborhood too; neighborhood=%q", synthUser)
	}
}

// TestAsk_ConfidenceFromNonRecallCitation: a sibling-expanded memory (not an
// original recall candidate, so no stored cosine) is the only source the answer
// cites. Before the fix it contributed nothing and confidence read 0; now its
// cosine to the question is computed by hydration, so the grounded answer scores
// above zero.
func TestAsk_ConfidenceFromNonRecallCitation(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingAskSiblingsPerCandidate, "global", "3")
	settings := NewSettingsService(repo)

	projects := askTestProjects()
	work := projects.bySlug["work"]
	cand := askCandidate("aaaaaaaa", "work", 0.7)
	cand.ProjectID = work.ID
	sib := model.Memory{ID: uuid.MustParse("ccccdddd-0000-0000-0000-000000000001"), NamespaceID: work.NamespaceID, Content: "ranshaw is a C++17 elliptic-curve library"}
	mem := &askFakeMem{siblings: []model.Memory{sib}}
	rc := &askFakeRecaller{resp: &RecallResponse{
		Memories:          []RecallResult{cand},
		QueryEmbedding:    []float32{1, 0},
		QueryEmbeddingDim: 2,
	}}
	// The sibling's embedding is aligned with the query (cosine 1.0), so it clears
	// the expansion floor and joins the neighborhood.
	vec := askFakeVectors{embs: map[uuid.UUID][]float32{sib.ID: {1, 0}}}
	// The answer cites ONLY the sibling, which has no original-recall cosine.
	llm := askFakeLLM{content: "ranshaw is C++ [ccccdddd]"}
	svc := newAskSvc(t, rc, mem, projects, llm, settings).WithVectorHydrator(vec)

	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "which projects are C++", ProjectSlug: "work", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Sources) != 1 {
		t.Fatalf("expected the sibling as the one cited source, got %d: %+v", len(resp.Sources), resp.Sources)
	}
	if resp.Sources[0].Score == nil {
		t.Error("a non-recall citation should now carry a question-relative cosine score")
	}
	if resp.Confidence <= 0 {
		t.Errorf("a grounded answer citing only a sibling source must score > 0 (was 0 before the fix), got %v", resp.Confidence)
	}
}

// TestAsk_ConfidenceRecallCitationUsesStoredCosine: a cited original recall hit
// keeps its stored VectorCosine and is NOT re-hydrated, so recall-cited answers
// score exactly as before and need no recalibration.
func TestAsk_ConfidenceRecallCitationUsesStoredCosine(t *testing.T) {
	projects := askTestProjects()
	work := projects.bySlug["work"]
	cand := askCandidate("aaaaaaaa", "work", 0.7) // stored VectorCosine 0.7
	cand.ProjectID = work.ID
	rc := &askFakeRecaller{resp: &RecallResponse{
		Memories:          []RecallResult{cand},
		QueryEmbedding:    []float32{1, 0},
		QueryEmbeddingDim: 2,
	}}
	// If the recall hit were (wrongly) re-hydrated, this orthogonal embedding would
	// drop its cosine to 0 and confidence to 0; the fix must keep the stored 0.7.
	vec := askFakeVectors{embs: map[uuid.UUID][]float32{cand.ID: {0, 1}}}
	llm := askFakeLLM{content: "answer [aaaaaaaa]"}
	svc := newAskSvc(t, rc, &askFakeMem{}, projects, llm, nil).WithVectorHydrator(vec)

	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "q", ProjectSlug: "work", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	want := askConfidence([]float64{0.7}, resp.Answer, 0.35, 0.75)
	if resp.Confidence != want {
		t.Errorf("recall hit must keep its stored cosine; got confidence %v, want %v (re-hydration would give 0)", resp.Confidence, want)
	}
	if resp.Confidence <= 0 {
		t.Errorf("expected positive confidence from the stored recall cosine, got %v", resp.Confidence)
	}
}

// TestAsk_ConfidenceUsesBestFacetForHydratedCite proves a hydrated (non-recall)
// cited source is scored on the best-facet scale, not the pooled facet-0 vector,
// when the hydrator implements bestFacetScorer. The sibling's pooled embedding is
// aligned with the query (cosine 1.0) so it clears the expansion gate and enters
// the neighborhood, but its best-facet cosine is a distinct 0.7; confidence must
// be built from 0.7 (the best-facet value), which it could only obtain via the
// BestFacetCosines path — the pooled path would have used 1.0.
func TestAsk_ConfidenceUsesBestFacetForHydratedCite(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingAskSiblingsPerCandidate, "global", "3")
	settings := NewSettingsService(repo)

	projects := askTestProjects()
	work := projects.bySlug["work"]
	cand := askCandidate("aaaaaaaa", "work", 0.7)
	cand.ProjectID = work.ID
	sib := model.Memory{ID: uuid.MustParse("ccccdddd-0000-0000-0000-000000000002"), NamespaceID: work.NamespaceID, Content: "ranshaw is a C++17 elliptic-curve library"}
	mem := &askFakeMem{siblings: []model.Memory{sib}}
	rc := &askFakeRecaller{resp: &RecallResponse{
		Memories:          []RecallResult{cand},
		QueryEmbedding:    []float32{1, 0},
		QueryEmbeddingDim: 2,
	}}
	// Pooled embedding clears the expansion gate (cosine 1.0); best-facet cosine is
	// a distinct 0.7 so the assertion pins down which path produced the score.
	vec := askFakeFacetVectors{
		askFakeVectors: askFakeVectors{embs: map[uuid.UUID][]float32{sib.ID: {1, 0}}},
		best:           map[uuid.UUID]float64{sib.ID: 0.7},
	}
	llm := askFakeLLM{content: "ranshaw is C++ [ccccdddd]"}
	svc := newAskSvc(t, rc, mem, projects, llm, settings).WithVectorHydrator(vec)

	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "which projects are C++", ProjectSlug: "work", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Sources) != 1 {
		t.Fatalf("expected the sibling as the one cited source, got %d: %+v", len(resp.Sources), resp.Sources)
	}
	if resp.Sources[0].Score == nil || *resp.Sources[0].Score != 0.7 {
		t.Fatalf("cited score must be the best-facet cosine 0.7, got %v", resp.Sources[0].Score)
	}
	want := askConfidence([]float64{0.7}, resp.Answer, 0.35, 0.75)
	if resp.Confidence != want {
		t.Errorf("confidence must be built from the best-facet cosine; got %v, want %v", resp.Confidence, want)
	}
}

// TestAsk_NonAggregationSingleRecall confirms an ordinary question is not
// decomposed: the decomposer returns an empty list, so exactly one recall runs
// and the path matches today's single-recall behavior.
func TestAsk_NonAggregationSingleRecall(t *testing.T) {
	rc := &askMapRecaller{
		byQuery: map[string]*RecallResponse{
			"what is the radius timeout": {Memories: []RecallResult{askCandidate("aaaaaaaa", "work", 0.8)}},
		},
		def: &RecallResponse{},
	}
	llm := &askSeqLLM{replies: []string{
		`{"subqueries":[]}`,
		"the timeout is 5s [aaaaaaaa].",
	}}
	svc := newAskSvc(t, &askFakeRecaller{}, &askFakeMem{}, askTestProjects(), llm, nil)
	svc.recall = rc

	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "what is the radius timeout", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatalf("ask: %v", err)
	}
	if resp.SynthesisMeta.SubqueryCount != 0 {
		t.Errorf("expected no decomposition, got SubqueryCount=%d", resp.SynthesisMeta.SubqueryCount)
	}
	if len(rc.queries) != 1 {
		t.Errorf("expected exactly 1 recall, got %d: %v", len(rc.queries), rc.queries)
	}
}
