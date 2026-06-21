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
	return RecallResult{ID: id, ProjectSlug: slug, Content: "memory content for " + prefix, Score: sim, Similarity: &s}
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

func TestAsk_SiblingsPulledIntoNeighborhood(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.put(SettingAskSiblingsPerCandidate, "global", "2")
	settings := NewSettingsService(repo)

	projects := askTestProjects()
	work := projects.bySlug["work"]
	cand := askCandidate("aaaaaaaa", "work", 0.8)
	cand.ProjectID = work.ID
	sib1 := model.Memory{ID: uuid.New(), NamespaceID: work.NamespaceID, Content: "sibling one"}
	sib2 := model.Memory{ID: uuid.New(), NamespaceID: work.NamespaceID, Content: "sibling two"}
	mem := &askFakeMem{siblings: []model.Memory{sib1, sib2}}
	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{cand}}}
	svc := newAskSvc(t, rc, mem, projects, askFakeLLM{content: "ok"}, settings)
	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "q", ProjectSlug: "work", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	// 1 candidate + 2 siblings = 3 in the neighborhood.
	if resp.SynthesisMeta.NeighborhoodSize != 3 {
		t.Errorf("expected 3 neighborhood members (candidate + 2 siblings), got %d", resp.SynthesisMeta.NeighborhoodSize)
	}
}

func TestAskConfidence(t *testing.T) {
	if c := askConfidence(nil, "x"); c != 0 {
		t.Errorf("no candidates → 0, got %v", c)
	}
	if c := askConfidence([]RecallResult{askCandidate("aaaaaaaa", "p", 0.9)}, "Not in neighborhood."); c != 0 {
		t.Errorf("not-in-neighborhood → 0, got %v", c)
	}
	c := askConfidence([]RecallResult{askCandidate("aaaaaaaa", "p", 0.9)}, "real answer")
	if c <= 0 || c > 1 {
		t.Errorf("confidence out of (0,1]: %v", c)
	}
}
