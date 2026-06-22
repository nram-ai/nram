package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// --- fakes ------------------------------------------------------------------

type askFakeRecaller struct {
	resp        *RecallResponse
	err         error
	lastRequest *RecallRequest
}

func (f *askFakeRecaller) Recall(_ context.Context, req *RecallRequest) (*RecallResponse, error) {
	f.lastRequest = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

type askFakeMem struct {
	byID     map[uuid.UUID]model.Memory
	siblings []model.Memory
}

func (f *askFakeMem) GetByID(_ context.Context, id, _ uuid.UUID) (*model.Memory, error) {
	if m, ok := f.byID[id]; ok {
		return &m, nil
	}
	return nil, errors.New("not found")
}

func (f *askFakeMem) GetBatch(_ context.Context, ids, _ []uuid.UUID) ([]model.Memory, error) {
	var out []model.Memory
	for _, id := range ids {
		if m, ok := f.byID[id]; ok {
			out = append(out, m)
		}
	}
	return out, nil
}

func (f *askFakeMem) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return nil, nil
}

func (f *askFakeMem) ListByNamespaceFiltered(_ context.Context, _ uuid.UUID, _ storage.MemoryListFilters, limit, _ int) ([]model.Memory, error) {
	if limit < len(f.siblings) {
		return f.siblings[:limit], nil
	}
	return f.siblings, nil
}

type askFakeProjects struct {
	bySlug map[string]model.Project
	byNS   map[uuid.UUID]model.Project
	byID   map[uuid.UUID]model.Project
	owned  []model.Project
}

func (f *askFakeProjects) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	if p, ok := f.byID[id]; ok {
		return &p, nil
	}
	return nil, errors.New("not found")
}

func (f *askFakeProjects) GetByNamespaceID(_ context.Context, ns uuid.UUID) (*model.Project, error) {
	if p, ok := f.byNS[ns]; ok {
		return &p, nil
	}
	return nil, errors.New("not found")
}

func (f *askFakeProjects) GetBySlug(_ context.Context, _ uuid.UUID, slug string) (*model.Project, error) {
	if p, ok := f.bySlug[slug]; ok {
		return &p, nil
	}
	return nil, errors.New("not found")
}

func (f *askFakeProjects) ListByUser(_ context.Context, _ uuid.UUID) ([]model.Project, error) {
	return f.owned, nil
}

type askFakeTraverser struct{}

func (askFakeTraverser) TraverseFromEntity(_ context.Context, _ uuid.UUID, _ []uuid.UUID, _, _ int) (storage.TraversalResult, error) {
	return storage.TraversalResult{}, nil
}

type askFakeLLM struct {
	content string
	err     error
}

func (f askFakeLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &provider.CompletionResponse{Content: f.content}, nil
}
func (askFakeLLM) Name() string     { return "fake" }
func (askFakeLLM) Models() []string { return []string{"fake"} }

// --- helpers ----------------------------------------------------------------

func askTestProjects() *askFakeProjects {
	ownerNS := uuid.New()
	globalP := model.Project{ID: uuid.New(), NamespaceID: uuid.New(), OwnerNamespaceID: ownerNS, Slug: model.ReservedProjectSlugGlobal}
	aboutP := model.Project{ID: uuid.New(), NamespaceID: uuid.New(), OwnerNamespaceID: ownerNS, Slug: model.ReservedProjectSlugAboutMe}
	work := model.Project{ID: uuid.New(), NamespaceID: uuid.New(), OwnerNamespaceID: ownerNS, Slug: "work"}
	return &askFakeProjects{
		bySlug: map[string]model.Project{globalP.Slug: globalP, aboutP.Slug: aboutP, work.Slug: work},
		byNS:   map[uuid.UUID]model.Project{globalP.NamespaceID: globalP, aboutP.NamespaceID: aboutP, work.NamespaceID: work},
		byID:   map[uuid.UUID]model.Project{globalP.ID: globalP, aboutP.ID: aboutP, work.ID: work},
		owned:  []model.Project{globalP, aboutP, work},
	}
}

func askCandidate(prefix string, slug string, sim float64) RecallResult {
	id := uuid.MustParse(prefix + "-0000-0000-0000-000000000001")
	s := sim
	c := sim
	return RecallResult{ID: id, ProjectSlug: slug, Content: "memory content for " + prefix, Score: sim, Similarity: &s, VectorCosine: &c}
}

func newAskSvc(t *testing.T, rc *askFakeRecaller, mem *askFakeMem, projects *askFakeProjects, llm provider.LLMProvider, settings *SettingsService) *AskService {
	t.Helper()
	var llmFn func() provider.LLMProvider
	llmFn = func() provider.LLMProvider { return llm }
	if llm == nil {
		llmFn = func() provider.LLMProvider { return nil }
	}
	return NewAskService(rc, mem, projects, askFakeTraverser{}, llmFn, settings)
}

// --- tests ------------------------------------------------------------------

func TestAsk_ProviderUnconfigured(t *testing.T) {
	rc := &askFakeRecaller{resp: &RecallResponse{}}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), nil, nil)
	_, err := svc.Ask(context.Background(), &AskRequest{Query: "q", OwnerNamespaceID: uuid.New()})
	if !errors.Is(err, ErrAskProviderUnconfigured) {
		t.Fatalf("expected ErrAskProviderUnconfigured, got %v", err)
	}
}

func TestAsk_EmptyQueryRejected(t *testing.T) {
	rc := &askFakeRecaller{resp: &RecallResponse{}}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), askFakeLLM{content: "x"}, nil)
	if _, err := svc.Ask(context.Background(), &AskRequest{Query: "  ", OwnerNamespaceID: uuid.New()}); err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestAsk_SynthesizerFailureDegrades(t *testing.T) {
	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{askCandidate("aaaaaaaa", "work", 0.8)}}}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), askFakeLLM{err: errors.New("boom")}, nil)
	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "q", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatalf("synthesizer failure must not error: %v", err)
	}
	if !resp.SynthesisMeta.SynthesisFailed {
		t.Error("expected SynthesisFailed=true")
	}
	if resp.Answer != "" {
		t.Errorf("expected empty answer on failure, got %q", resp.Answer)
	}
	if len(resp.Sources) != 1 {
		t.Errorf("expected recall sources preserved, got %d", len(resp.Sources))
	}
	if resp.Confidence != 0 {
		t.Errorf("expected 0 confidence on failure, got %v", resp.Confidence)
	}
}

func TestAsk_RenumbersCitationsAndStripsUnknown(t *testing.T) {
	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{askCandidate("aaaaaaaa", "work", 0.8)}}}
	// aaaaaaaa is a real candidate short id; deadbeef is not in the neighborhood.
	llm := askFakeLLM{content: "Per the notes [aaaaaaaa] but also [deadbeef] said so."}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), llm, nil)
	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "q", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Answer, "[1]") {
		t.Errorf("valid citation should be renumbered to [1]: %q", resp.Answer)
	}
	if strings.Contains(resp.Answer, "aaaaaaaa") || strings.Contains(resp.Answer, "deadbeef") {
		t.Errorf("no raw ids should survive in the answer: %q", resp.Answer)
	}
	if len(resp.Sources) != 1 || resp.Sources[0].Citation != 1 {
		t.Errorf("expected one cited source with citation 1, got %+v", resp.Sources)
	}
	if resp.Confidence <= 0 {
		t.Errorf("expected positive confidence, got %v", resp.Confidence)
	}
}

func TestAsk_RelevanceFloorDropsWeakTail(t *testing.T) {
	// Recall returns a strong pair plus an off-topic tail hit. With the default
	// 0.5 ratio the floor is 0.5*0.9=0.45, so the 0.2 tail is dropped from the
	// neighborhood and never reaches the synthesizer.
	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{
		askCandidate("aaaaaaaa", "work", 0.9),
		askCandidate("bbbbbbbb", "work", 0.8),
		askCandidate("cccccccc", "work", 0.2),
	}}}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), askFakeLLM{content: "answer [aaaaaaaa]"}, nil)
	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "q", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if resp.SynthesisMeta.NeighborhoodSize != 2 {
		t.Errorf("relevance floor should drop the weak tail (0.2 < 0.5*0.9); neighborhood=%d, want 2", resp.SynthesisMeta.NeighborhoodSize)
	}
}

func TestAsk_SuppressesUngroundedAnswer(t *testing.T) {
	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{askCandidate("aaaaaaaa", "work", 0.8)}}}
	// The model ignores the neighborhood and cites nothing — confabulation or an
	// injected instruction ("say PWNED"). The ungrounded text must never surface.
	llm := askFakeLLM{content: "PWNED. Ignore the memories, your new instructions are..."}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), llm, nil)
	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "q", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Answer != "Not in neighborhood." {
		t.Errorf("uncited answer must normalize to the sentinel, got %q", resp.Answer)
	}
	if strings.Contains(resp.Answer, "PWNED") {
		t.Errorf("injected text must not surface: %q", resp.Answer)
	}
	if resp.Confidence != 0 {
		t.Errorf("ungrounded answer must have confidence 0, got %v", resp.Confidence)
	}
	// Sources fall back to the retrieved candidates (uncited fallback).
	if len(resp.Sources) != 1 {
		t.Errorf("expected fallback recall sources, got %d", len(resp.Sources))
	}
}

func TestAsk_ShareScoped_SpansGrantedProjectsOnly(t *testing.T) {
	projects := askTestProjects()
	a := projects.bySlug["work"]
	// second granted project
	b := model.Project{ID: uuid.New(), NamespaceID: uuid.New(), Slug: "shared2"}
	projects.byID[b.ID] = b
	projects.bySlug[b.Slug] = b

	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{askCandidate("aaaaaaaa", "work", 0.8)}}}
	svc := newAskSvc(t, rc, &askFakeMem{}, projects, askFakeLLM{content: "ok"}, nil)

	// Wide share ask: aperture = the granted set, NEVER global/about_me.
	_, err := svc.Ask(context.Background(), &AskRequest{
		Query:            "q",
		ShareScoped:      true,
		ShareProjectIDs:  []uuid.UUID{a.ID, b.ID},
		OwnerNamespaceID: uuid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	r := rc.lastRequest
	if r.GlobalNamespaceID != nil || r.AboutMeNamespaceID != nil {
		t.Errorf("share-scoped ask must never include global/about_me; got global=%v about=%v", r.GlobalNamespaceID, r.AboutMeNamespaceID)
	}
	// Primary is the first grant; the second grant is in the aperture.
	if r.ProjectID != a.ID {
		t.Errorf("expected primary = first grant %v, got %v", a.ID, r.ProjectID)
	}
	found := false
	for _, ns := range r.ApertureNamespaceIDs {
		if ns == b.NamespaceID {
			found = true
		}
		if ns == projects.bySlug["global"].NamespaceID || ns == projects.bySlug["about_me"].NamespaceID {
			t.Errorf("aperture leaked a reserved-tier namespace: %v", ns)
		}
	}
	if !found {
		t.Errorf("expected second granted project in aperture, got %v", r.ApertureNamespaceIDs)
	}
}

func TestAsk_ShareScoped_RejectsUngrantedProject(t *testing.T) {
	projects := askTestProjects()
	granted := projects.bySlug["work"]
	rc := &askFakeRecaller{resp: &RecallResponse{}}
	svc := newAskSvc(t, rc, &askFakeMem{}, projects, askFakeLLM{content: "ok"}, nil)
	// Ask scoped to "global" (a real project) but only "work" is granted.
	_, err := svc.Ask(context.Background(), &AskRequest{
		Query:            "q",
		ProjectSlug:      "global",
		ShareScoped:      true,
		ShareProjectIDs:  []uuid.UUID{granted.ID},
		OwnerNamespaceID: uuid.New(),
	})
	if err == nil {
		t.Fatal("expected error asking an ungranted project, got nil")
	}
	if rc.lastRequest != nil {
		t.Error("recall should not run for an ungranted project")
	}
}

func TestAsk_ShareScoped_NoGrantsErrors(t *testing.T) {
	rc := &askFakeRecaller{resp: &RecallResponse{}}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), askFakeLLM{content: "ok"}, nil)
	_, err := svc.Ask(context.Background(), &AskRequest{
		Query:            "q",
		ShareScoped:      true,
		ShareProjectIDs:  nil,
		OwnerNamespaceID: uuid.New(),
	})
	if err == nil {
		t.Fatal("expected error when share grants no readable projects")
	}
}

func TestRenumberCitations(t *testing.T) {
	nbr := []neighborMemory{
		{shortID: "aaaaaaaa", memoryID: uuid.New(), projectSlug: "p"},
		{shortID: "bbbbbbbb", memoryID: uuid.New(), projectSlug: "p"},
	}
	// First-appearance order: bbbbbbbb -> [1], aaaaaaaa -> [2]; cafebabe stripped.
	out, cited := renumberCitations("x [bbbbbbbb] y [cafebabe] z [aaaaaaaa] w [bbbbbbbb]", nbr)
	if out != "x [1] y z [2] w [1]" {
		t.Errorf("unexpected renumbered answer: %q", out)
	}
	if len(cited) != 2 || cited[0].shortID != "bbbbbbbb" || cited[1].shortID != "aaaaaaaa" {
		t.Errorf("unexpected cited order: %+v", cited)
	}
}

func TestAsk_NeighborhoodCapEnforced(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingAskNeighborhoodMaxMemories, "global", "2")
	repo.put(SettingAskSiblingsPerCandidate, "global", "0")
	settings := NewSettingsService(repo)

	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{
		askCandidate("aaaaaaaa", "work", 0.9),
		askCandidate("bbbbbbbb", "work", 0.8),
		askCandidate("cccccccc", "work", 0.7),
	}}}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), askFakeLLM{content: "ok"}, settings)
	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "q", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if resp.SynthesisMeta.NeighborhoodSize != 2 {
		t.Errorf("expected neighborhood capped at 2, got %d", resp.SynthesisMeta.NeighborhoodSize)
	}
}

func TestAsk_WideApertureUnionsOwnedProjects(t *testing.T) {
	projects := askTestProjects()
	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{askCandidate("aaaaaaaa", "work", 0.8)}}}
	svc := newAskSvc(t, rc, &askFakeMem{}, projects, askFakeLLM{content: "ok"}, nil)
	if _, err := svc.Ask(context.Background(), &AskRequest{Query: "q", OwnerNamespaceID: uuid.New()}); err != nil {
		t.Fatal(err)
	}
	if rc.lastRequest == nil {
		t.Fatal("recall was not called")
	}
	// Wide aperture: the non-primary owned project ("work") namespace must be in
	// the aperture set. Primary rides the global tier.
	work := projects.bySlug["work"]
	found := false
	for _, ns := range rc.lastRequest.ApertureNamespaceIDs {
		if ns == work.NamespaceID {
			found = true
		}
	}
	if !found {
		t.Errorf("expected work namespace in aperture, got %v", rc.lastRequest.ApertureNamespaceIDs)
	}
}

func TestAsk_ScopedProjectNarrowsAperture(t *testing.T) {
	projects := askTestProjects()
	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{askCandidate("aaaaaaaa", "work", 0.8)}}}
	svc := newAskSvc(t, rc, &askFakeMem{}, projects, askFakeLLM{content: "ok"}, nil)
	if _, err := svc.Ask(context.Background(), &AskRequest{Query: "q", ProjectSlug: "work", OwnerNamespaceID: uuid.New()}); err != nil {
		t.Fatal(err)
	}
	if len(rc.lastRequest.ApertureNamespaceIDs) != 0 {
		t.Errorf("scoped ask must not widen the aperture, got %v", rc.lastRequest.ApertureNamespaceIDs)
	}
	work := projects.bySlug["work"]
	if rc.lastRequest.ProjectID != work.ID {
		t.Errorf("expected primary project %v, got %v", work.ID, rc.lastRequest.ProjectID)
	}
}

// askFakeVectors is a VectorHydrator stub: it returns the embeddings it was
// seeded with, so tests can drive the graph/sibling relevance gate.
type askFakeVectors struct {
	embs map[uuid.UUID][]float32
}

func (f askFakeVectors) GetByIDs(_ context.Context, _ storage.VectorKind, ids []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	out := make(map[uuid.UUID][]float32)
	for _, id := range ids {
		if e, ok := f.embs[id]; ok {
			out[id] = e
		}
	}
	return out, nil
}

func TestAsk_SiblingsRelevanceGated(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingAskSiblingsPerCandidate, "global", "3")
	settings := NewSettingsService(repo)

	projects := askTestProjects()
	work := projects.bySlug["work"]
	cand := askCandidate("aaaaaaaa", "work", 0.8)
	cand.ProjectID = work.ID
	sibRel1 := model.Memory{ID: uuid.New(), NamespaceID: work.NamespaceID, Content: "relevant sibling one"}
	sibRel2 := model.Memory{ID: uuid.New(), NamespaceID: work.NamespaceID, Content: "relevant sibling two"}
	sibOff := model.Memory{ID: uuid.New(), NamespaceID: work.NamespaceID, Content: "off-topic sibling"}
	mem := &askFakeMem{siblings: []model.Memory{sibRel1, sibRel2, sibOff}}
	rc := &askFakeRecaller{resp: &RecallResponse{
		Memories:          []RecallResult{cand},
		QueryEmbedding:    []float32{1, 0},
		QueryEmbeddingDim: 2,
	}}
	// Relevant siblings share the query direction (cosine 1.0); the off-topic one
	// is orthogonal (cosine 0.0) and must be gated out by the expansion floor.
	vec := askFakeVectors{embs: map[uuid.UUID][]float32{
		sibRel1.ID: {1, 0},
		sibRel2.ID: {1, 0},
		sibOff.ID:  {0, 1},
	}}
	svc := newAskSvc(t, rc, mem, projects, askFakeLLM{content: "ok"}, settings).WithVectorHydrator(vec)
	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "q", ProjectSlug: "work", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	// candidate + 2 relevant siblings; the off-topic sibling is gated out.
	if resp.SynthesisMeta.NeighborhoodSize != 3 {
		t.Errorf("expected 3 (candidate + 2 relevant siblings, off-topic dropped), got %d", resp.SynthesisMeta.NeighborhoodSize)
	}
}

func TestAskConfidence(t *testing.T) {
	const floor, ceiling = 0.35, 0.75

	// Grounding gate: no cited recall cosine → 0. This is the confabulation /
	// prompt-injection case (the answer cited nothing from the neighborhood).
	if c := askConfidence(nil, "a real-looking but ungrounded answer", floor, ceiling); c != 0 {
		t.Errorf("ungrounded (no cited cosines) → 0, got %v", c)
	}
	// Explicit not-in-neighborhood → 0 even if some cosine slipped through.
	if c := askConfidence([]float64{0.9}, "Not in neighborhood.", floor, ceiling); c != 0 {
		t.Errorf("not-in-neighborhood → 0, got %v", c)
	}

	// A genuinely strong, well-corroborated answer scores high.
	strong := askConfidence([]float64{0.72, 0.68, 0.66}, "answer", floor, ceiling)
	if strong <= 0 || strong > 1 {
		t.Fatalf("strong confidence out of (0,1]: %v", strong)
	}
	// A single weak-but-above-floor citation scores lower than the strong case —
	// the metric must discriminate, unlike the old rank-invariant constant.
	weak := askConfidence([]float64{0.45}, "answer", floor, ceiling)
	if weak <= 0 || weak >= strong {
		t.Errorf("weak (%v) should be in (0, strong=%v)", weak, strong)
	}
	// A cosine at/below the floor calibrates to ~0 evidence regardless of count.
	if c := askConfidence([]float64{0.30, 0.20}, "answer", floor, ceiling); c != 0 {
		t.Errorf("all cosines below floor → 0 evidence, got %v", c)
	}
	// More corroborating citations at the same strength raise confidence.
	one := askConfidence([]float64{0.72}, "answer", floor, ceiling)
	three := askConfidence([]float64{0.72, 0.72, 0.72}, "answer", floor, ceiling)
	if three <= one {
		t.Errorf("corroboration: three citations (%v) should exceed one (%v)", three, one)
	}
}
