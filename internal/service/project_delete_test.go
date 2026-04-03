package service

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- Mock implementations ---

type mockProjectDeleteGetter struct {
	projects map[uuid.UUID]*model.Project
	slugMap  map[string]*model.Project // key: ownerNS+slug
}

func (m *mockProjectDeleteGetter) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	p, ok := m.projects[id]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return p, nil
}

func (m *mockProjectDeleteGetter) GetBySlug(_ context.Context, ownerNamespaceID uuid.UUID, slug string) (*model.Project, error) {
	key := ownerNamespaceID.String() + "/" + slug
	p, ok := m.slugMap[key]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return p, nil
}

type mockProjectDeleter struct {
	deleted []uuid.UUID
}

func (m *mockProjectDeleter) Delete(_ context.Context, id uuid.UUID) error {
	m.deleted = append(m.deleted, id)
	return nil
}

type mockMemoryIDLister struct {
	ids []uuid.UUID
}

func (m *mockMemoryIDLister) ListIDsByNamespace(_ context.Context, _ uuid.UUID) ([]uuid.UUID, error) {
	return m.ids, nil
}

type mockMemoryBulkDeleter struct {
	called bool
}

func (m *mockMemoryBulkDeleter) HardDeleteByNamespace(_ context.Context, _ uuid.UUID) error {
	m.called = true
	return nil
}

type mockPDVectorDeleter struct {
	deleted []uuid.UUID
}

func (m *mockPDVectorDeleter) Delete(_ context.Context, id uuid.UUID) error {
	m.deleted = append(m.deleted, id)
	return nil
}

type mockEntityBulkDeleter struct {
	called    bool
	returnErr error
}

func (m *mockEntityBulkDeleter) DeleteByNamespace(_ context.Context, _ uuid.UUID) error {
	m.called = true
	return m.returnErr
}

type mockRelationshipBulkDeleter struct {
	called bool
}

func (m *mockRelationshipBulkDeleter) DeleteByNamespace(_ context.Context, _ uuid.UUID) error {
	m.called = true
	return nil
}

type mockRelCleaner struct {
	called []uuid.UUID
}

func (m *mockRelCleaner) DeleteBySourceMemory(_ context.Context, _ uuid.UUID, memoryID uuid.UUID) error {
	m.called = append(m.called, memoryID)
	return nil
}

type mockLineageCleaner struct {
	called []uuid.UUID
}

func (m *mockLineageCleaner) DeleteByMemoryID(_ context.Context, _ uuid.UUID, memoryID uuid.UUID) error {
	m.called = append(m.called, memoryID)
	return nil
}

type mockEnrichmentBulkDeleter struct {
	byMemory    []uuid.UUID
	byNamespace bool
}

func (m *mockEnrichmentBulkDeleter) DeleteByMemoryID(_ context.Context, memoryID uuid.UUID) error {
	m.byMemory = append(m.byMemory, memoryID)
	return nil
}

func (m *mockEnrichmentBulkDeleter) DeleteByNamespace(_ context.Context, _ uuid.UUID) error {
	m.byNamespace = true
	return nil
}

type mockTokenUsageReassigner struct {
	from uuid.UUID
	to   uuid.UUID
}

func (m *mockTokenUsageReassigner) ReassignProject(_ context.Context, from, to uuid.UUID, _ uuid.UUID) error {
	m.from = from
	m.to = to
	return nil
}

type mockTokenUsageCleaner struct {
	called []uuid.UUID
}

func (m *mockTokenUsageCleaner) DeleteByMemoryID(_ context.Context, memoryID uuid.UUID) error {
	m.called = append(m.called, memoryID)
	return nil
}

type mockIngestionLogDeleter struct {
	called bool
}

func (m *mockIngestionLogDeleter) DeleteByNamespace(_ context.Context, _ uuid.UUID) error {
	m.called = true
	return nil
}

type mockShareDeleter struct {
	called bool
}

func (m *mockShareDeleter) DeleteByNamespace(_ context.Context, _ uuid.UUID) error {
	m.called = true
	return nil
}

type mockNamespaceDeleter struct {
	deleted []uuid.UUID
}

func (m *mockNamespaceDeleter) Delete(_ context.Context, id uuid.UUID) error {
	m.deleted = append(m.deleted, id)
	return nil
}

// --- Tests ---

func TestProjectDeleteService_SuccessfulCascade(t *testing.T) {
	projectID := uuid.New()
	nsID := uuid.New()
	ownerNS := uuid.New()
	globalProjectID := uuid.New()
	globalNSID := uuid.New()
	mem1 := uuid.New()
	mem2 := uuid.New()

	project := &model.Project{
		ID:               projectID,
		NamespaceID:      nsID,
		OwnerNamespaceID: ownerNS,
		Slug:             "test-project",
	}
	globalProject := &model.Project{
		ID:               globalProjectID,
		NamespaceID:      globalNSID,
		OwnerNamespaceID: ownerNS,
		Slug:             "global",
	}

	getter := &mockProjectDeleteGetter{
		projects: map[uuid.UUID]*model.Project{projectID: project},
		slugMap:  map[string]*model.Project{ownerNS.String() + "/global": globalProject},
	}
	deleter := &mockProjectDeleter{}
	memLister := &mockMemoryIDLister{ids: []uuid.UUID{mem1, mem2}}
	memBulk := &mockMemoryBulkDeleter{}
	vecDel := &mockPDVectorDeleter{}
	entDel := &mockEntityBulkDeleter{}
	relBulk := &mockRelationshipBulkDeleter{}
	relClean := &mockRelCleaner{}
	linClean := &mockLineageCleaner{}
	enrDel := &mockEnrichmentBulkDeleter{}
	tokReassign := &mockTokenUsageReassigner{}
	tokClean := &mockTokenUsageCleaner{}
	ingDel := &mockIngestionLogDeleter{}
	shareDel := &mockShareDeleter{}
	nsDel := &mockNamespaceDeleter{}

	svc := NewProjectDeleteService(
		getter, deleter, memLister, memBulk, vecDel,
		entDel, relBulk, relClean, linClean, enrDel,
		tokReassign, tokClean, ingDel, shareDel, nil, nsDel, nil,
	)

	resp, err := svc.Delete(context.Background(), &ProjectDeleteRequest{ProjectID: projectID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.DeletedMemories != 2 {
		t.Errorf("expected 2 deleted memories, got %d", resp.DeletedMemories)
	}
	if resp.ProjectSlug != "test-project" {
		t.Errorf("expected slug test-project, got %s", resp.ProjectSlug)
	}

	// Verify all cleanup methods were called.
	if len(relClean.called) != 2 {
		t.Errorf("expected 2 relationship cleanup calls, got %d", len(relClean.called))
	}
	if len(linClean.called) != 2 {
		t.Errorf("expected 2 lineage cleanup calls, got %d", len(linClean.called))
	}
	if len(enrDel.byMemory) != 2 {
		t.Errorf("expected 2 enrichment per-memory calls, got %d", len(enrDel.byMemory))
	}
	if len(tokClean.called) != 2 {
		t.Errorf("expected 2 token usage cleanup calls, got %d", len(tokClean.called))
	}
	if len(vecDel.deleted) != 2 {
		t.Errorf("expected 2 vector deletes, got %d", len(vecDel.deleted))
	}
	if !memBulk.called {
		t.Error("expected bulk memory delete to be called")
	}
	if !entDel.called {
		t.Error("expected entity delete to be called")
	}
	if !relBulk.called {
		t.Error("expected relationship bulk delete to be called")
	}
	if !ingDel.called {
		t.Error("expected ingestion log delete to be called")
	}
	if !shareDel.called {
		t.Error("expected share delete to be called")
	}
	if !enrDel.byNamespace {
		t.Error("expected enrichment namespace delete to be called")
	}
	if tokReassign.from != projectID || tokReassign.to != globalProjectID {
		t.Errorf("expected token reassign from %s to %s, got from %s to %s",
			projectID, globalProjectID, tokReassign.from, tokReassign.to)
	}
	if len(deleter.deleted) != 1 || deleter.deleted[0] != projectID {
		t.Errorf("expected project %s to be deleted", projectID)
	}
	if len(nsDel.deleted) != 1 || nsDel.deleted[0] != nsID {
		t.Errorf("expected namespace %s to be deleted", nsID)
	}
}

func TestProjectDeleteService_RejectsGlobal(t *testing.T) {
	projectID := uuid.New()
	project := &model.Project{
		ID:   projectID,
		Slug: "global",
	}
	getter := &mockProjectDeleteGetter{
		projects: map[uuid.UUID]*model.Project{projectID: project},
	}
	svc := NewProjectDeleteService(
		getter, &mockProjectDeleter{}, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)

	_, err := svc.Delete(context.Background(), &ProjectDeleteRequest{ProjectID: projectID})
	if err == nil {
		t.Fatal("expected error for global project")
	}
	if err.Error() != "the global project cannot be deleted" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProjectDeleteService_ZeroMemories(t *testing.T) {
	projectID := uuid.New()
	nsID := uuid.New()
	ownerNS := uuid.New()
	globalProjectID := uuid.New()

	project := &model.Project{
		ID:               projectID,
		NamespaceID:      nsID,
		OwnerNamespaceID: ownerNS,
		Slug:             "empty-project",
	}
	globalProject := &model.Project{
		ID:               globalProjectID,
		OwnerNamespaceID: ownerNS,
		Slug:             "global",
	}

	getter := &mockProjectDeleteGetter{
		projects: map[uuid.UUID]*model.Project{projectID: project},
		slugMap:  map[string]*model.Project{ownerNS.String() + "/global": globalProject},
	}
	deleter := &mockProjectDeleter{}
	memLister := &mockMemoryIDLister{ids: nil}
	memBulk := &mockMemoryBulkDeleter{}

	svc := NewProjectDeleteService(
		getter, deleter, memLister, memBulk, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &mockNamespaceDeleter{}, nil,
	)

	resp, err := svc.Delete(context.Background(), &ProjectDeleteRequest{ProjectID: projectID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.DeletedMemories != 0 {
		t.Errorf("expected 0 deleted memories, got %d", resp.DeletedMemories)
	}
	if len(deleter.deleted) != 1 {
		t.Error("expected project to be deleted even with zero memories")
	}
}

func TestProjectDeleteService_TokenUsageReassigned(t *testing.T) {
	projectID := uuid.New()
	ownerNS := uuid.New()
	globalProjectID := uuid.New()

	project := &model.Project{
		ID:               projectID,
		NamespaceID:      uuid.New(),
		OwnerNamespaceID: ownerNS,
		Slug:             "my-project",
	}
	globalProject := &model.Project{
		ID:               globalProjectID,
		OwnerNamespaceID: ownerNS,
		Slug:             "global",
	}

	getter := &mockProjectDeleteGetter{
		projects: map[uuid.UUID]*model.Project{projectID: project},
		slugMap:  map[string]*model.Project{ownerNS.String() + "/global": globalProject},
	}
	tokReassign := &mockTokenUsageReassigner{}

	svc := NewProjectDeleteService(
		getter, &mockProjectDeleter{}, &mockMemoryIDLister{}, &mockMemoryBulkDeleter{}, nil,
		nil, nil, nil, nil, nil, tokReassign, nil, nil, nil, nil, &mockNamespaceDeleter{}, nil,
	)

	_, err := svc.Delete(context.Background(), &ProjectDeleteRequest{ProjectID: projectID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tokReassign.from != projectID {
		t.Errorf("expected from=%s, got %s", projectID, tokReassign.from)
	}
	if tokReassign.to != globalProjectID {
		t.Errorf("expected to=%s, got %s", globalProjectID, tokReassign.to)
	}
}

func TestProjectDeleteService_PartialFailure(t *testing.T) {
	projectID := uuid.New()
	nsID := uuid.New()
	ownerNS := uuid.New()
	mem1 := uuid.New()

	project := &model.Project{
		ID:               projectID,
		NamespaceID:      nsID,
		OwnerNamespaceID: ownerNS,
		Slug:             "failing-project",
	}

	getter := &mockProjectDeleteGetter{
		projects: map[uuid.UUID]*model.Project{projectID: project},
		slugMap:  map[string]*model.Project{},
	}
	deleter := &mockProjectDeleter{}
	memLister := &mockMemoryIDLister{ids: []uuid.UUID{mem1}}
	memBulk := &mockMemoryBulkDeleter{}
	entDel := &mockEntityBulkDeleter{returnErr: fmt.Errorf("entity cleanup failed")}
	relBulk := &mockRelationshipBulkDeleter{}

	svc := NewProjectDeleteService(
		getter, deleter, memLister, memBulk, nil,
		entDel, relBulk, nil, nil, nil, nil, nil, nil, nil, nil, &mockNamespaceDeleter{}, nil,
	)

	resp, err := svc.Delete(context.Background(), &ProjectDeleteRequest{ProjectID: projectID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Entity failure should not prevent project deletion.
	if resp.DeletedMemories != 1 {
		t.Errorf("expected 1 deleted memory, got %d", resp.DeletedMemories)
	}
	if len(deleter.deleted) != 1 {
		t.Error("expected project to be deleted despite entity cleanup failure")
	}
	if !entDel.called {
		t.Error("expected entity delete to be attempted")
	}
}

func TestProjectDeleteService_ProjectNotFound(t *testing.T) {
	getter := &mockProjectDeleteGetter{
		projects: map[uuid.UUID]*model.Project{},
	}
	svc := NewProjectDeleteService(
		getter, &mockProjectDeleter{}, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)

	_, err := svc.Delete(context.Background(), &ProjectDeleteRequest{ProjectID: uuid.New()})
	if err == nil {
		t.Fatal("expected error for missing project")
	}
}
