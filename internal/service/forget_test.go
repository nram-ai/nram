package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// --- Forget mock implementations ---

type mockMemoryDeleter struct {
	memories    map[uuid.UUID]*model.Memory
	nsList      []model.Memory
	softDeleted map[uuid.UUID]bool
	hardDeleted map[uuid.UUID]bool
	listErr     error
}

func newMockMemoryDeleter() *mockMemoryDeleter {
	return &mockMemoryDeleter{
		memories:    make(map[uuid.UUID]*model.Memory),
		softDeleted: make(map[uuid.UUID]bool),
		hardDeleted: make(map[uuid.UUID]bool),
	}
}

func (m *mockMemoryDeleter) FindBySupersededBy(_ context.Context, _ uuid.UUID, id uuid.UUID) ([]uuid.UUID, error) {
	var out []uuid.UUID
	for ancestorID, mem := range m.memories {
		if mem.SupersededBy != nil && *mem.SupersededBy == id && mem.DeletedAt == nil {
			out = append(out, ancestorID)
		}
	}
	return out, nil
}

func (m *mockMemoryDeleter) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mem, nil
}

func (m *mockMemoryDeleter) ListByNamespace(_ context.Context, _ uuid.UUID, limit, offset int) ([]model.Memory, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	if offset >= len(m.nsList) {
		return nil, nil
	}
	end := offset + limit
	if end > len(m.nsList) {
		end = len(m.nsList)
	}
	return m.nsList[offset:end], nil
}

func (m *mockMemoryDeleter) SoftDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	m.softDeleted[id] = true
	return nil
}

func (m *mockMemoryDeleter) HardDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	m.hardDeleted[id] = true
	return nil
}

type mockVectorDeleter struct {
	deleted map[uuid.UUID]bool
}

func newMockVectorDeleter() *mockVectorDeleter {
	return &mockVectorDeleter{deleted: make(map[uuid.UUID]bool)}
}

func (m *mockVectorDeleter) Delete(_ context.Context, _ storage.VectorKind, id uuid.UUID) error {
	m.deleted[id] = true
	return nil
}

type mockForgetProjectRepo struct {
	projects map[uuid.UUID]*model.Project
}

func (m *mockForgetProjectRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	p, ok := m.projects[id]
	if !ok {
		return nil, fmt.Errorf("project not found")
	}
	return p, nil
}

func (m *mockForgetProjectRepo) GetByNamespaceID(_ context.Context, namespaceID uuid.UUID) (*model.Project, error) {
	for _, p := range m.projects {
		if p.NamespaceID == namespaceID {
			return p, nil
		}
	}
	return nil, fmt.Errorf("project not found for namespace")
}

// --- Helper to build test fixtures ---

func forgetTestFixtures() (uuid.UUID, uuid.UUID, uuid.UUID, *model.Project) {
	projectID := uuid.New()
	namespaceID := uuid.New()
	memoryID := uuid.New()
	project := &model.Project{
		ID:          projectID,
		NamespaceID: namespaceID,
		Name:        "test-project",
		Slug:        "test-project",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	return projectID, namespaceID, memoryID, project
}

func makeMemory(id, namespaceID uuid.UUID, tags []string) *model.Memory {
	return &model.Memory{
		ID:          id,
		NamespaceID: namespaceID,
		Content:     "test content",
		Tags:        tags,
		Confidence:  1.0,
		Importance:  0.5,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}

// --- Tests ---

func TestForget_SingleSoftDelete(t *testing.T) {
	projectID, nsID, memID, project := forgetTestFixtures()

	deleter := newMockMemoryDeleter()
	deleter.memories[memID] = makeMemory(memID, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryID:  &memID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", resp.Deleted)
	}
	if !deleter.softDeleted[memID] {
		t.Fatal("expected memory to be soft deleted")
	}
	if deleter.hardDeleted[memID] {
		t.Fatal("memory should not be hard deleted")
	}
}

func TestForget_SingleHardDelete(t *testing.T) {
	projectID, nsID, memID, project := forgetTestFixtures()

	deleter := newMockMemoryDeleter()
	deleter.memories[memID] = makeMemory(memID, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID:  projectID,
		MemoryID:   &memID,
		HardDelete: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", resp.Deleted)
	}
	if !deleter.hardDeleted[memID] {
		t.Fatal("expected memory to be hard deleted")
	}
}

func TestForget_HardDeleteRemovesFromVectorStore(t *testing.T) {
	projectID, nsID, memID, project := forgetTestFixtures()

	deleter := newMockMemoryDeleter()
	deleter.memories[memID] = makeMemory(memID, nsID, nil)

	vectorDeleter := newMockVectorDeleter()
	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, vectorDeleter, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID:  projectID,
		MemoryID:   &memID,
		HardDelete: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", resp.Deleted)
	}
	if !vectorDeleter.deleted[memID] {
		t.Fatal("expected memory to be deleted from vector store")
	}
}

func TestForget_BulkDelete(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	deleter := newMockMemoryDeleter()
	deleter.memories[id1] = makeMemory(id1, nsID, nil)
	deleter.memories[id2] = makeMemory(id2, nsID, nil)
	deleter.memories[id3] = makeMemory(id3, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryIDs: []uuid.UUID{id1, id2, id3},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 3 {
		t.Fatalf("expected 3 deleted, got %d", resp.Deleted)
	}
	for _, id := range []uuid.UUID{id1, id2, id3} {
		if !deleter.softDeleted[id] {
			t.Fatalf("expected %s to be soft deleted", id)
		}
	}
}

func TestForget_TagBasedDelete(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	mem1 := makeMemory(id1, nsID, []string{"important", "work"})
	mem2 := makeMemory(id2, nsID, []string{"important"})
	mem3 := makeMemory(id3, nsID, []string{"personal"})

	deleter := newMockMemoryDeleter()
	deleter.memories[id1] = mem1
	deleter.memories[id2] = mem2
	deleter.memories[id3] = mem3
	deleter.nsList = []model.Memory{*mem1, *mem2, *mem3}

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	// Delete all memories tagged "important".
	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		Tags:      []string{"important"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("expected 2 deleted, got %d", resp.Deleted)
	}
	if !deleter.softDeleted[id1] {
		t.Fatal("expected mem1 to be deleted")
	}
	if !deleter.softDeleted[id2] {
		t.Fatal("expected mem2 to be deleted")
	}
	if deleter.softDeleted[id3] {
		t.Fatal("mem3 should not be deleted")
	}
}

func TestForget_ProjectNotFound(t *testing.T) {
	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{}}
	svc := NewForgetService(newMockMemoryDeleter(), projects, nil, nil)

	memID := uuid.New()
	_, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: uuid.New(),
		MemoryID:  &memID,
	})
	if err == nil {
		t.Fatal("expected error for missing project")
	}
}

func TestForget_MemoryNotFound_SkipsGracefully(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	existingID := uuid.New()
	missingID := uuid.New()

	deleter := newMockMemoryDeleter()
	deleter.memories[existingID] = makeMemory(existingID, nsID, nil)
	// missingID is not in the map

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryIDs: []uuid.UUID{existingID, missingID},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 1 {
		t.Fatalf("expected 1 deleted (skipping missing), got %d", resp.Deleted)
	}
}

func TestForget_NoFilterProvided(t *testing.T) {
	projectID, _, _, project := forgetTestFixtures()

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(newMockMemoryDeleter(), projects, nil, nil)

	_, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
	})
	if err == nil {
		t.Fatal("expected error when no filter provided")
	}
}

func TestForget_EmptyMemoryIDsList(t *testing.T) {
	projectID, _, _, project := forgetTestFixtures()

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(newMockMemoryDeleter(), projects, nil, nil)

	// Empty slice = no filter provided.
	_, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryIDs: []uuid.UUID{},
	})
	if err == nil {
		t.Fatal("expected error when memory_ids is empty")
	}
}

func TestForget_LatencyTracked(t *testing.T) {
	projectID, nsID, memID, project := forgetTestFixtures()

	deleter := newMockMemoryDeleter()
	deleter.memories[memID] = makeMemory(memID, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryID:  &memID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.LatencyMs < 0 {
		t.Fatalf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

func TestForget_NilVectorStoreNoPanic(t *testing.T) {
	projectID, nsID, memID, project := forgetTestFixtures()

	deleter := newMockMemoryDeleter()
	deleter.memories[memID] = makeMemory(memID, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	// vectorStore is nil.
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID:  projectID,
		MemoryID:   &memID,
		HardDelete: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", resp.Deleted)
	}
	if !deleter.hardDeleted[memID] {
		t.Fatal("expected memory to be hard deleted")
	}
}

func TestForget_MemoryWrongNamespace_Skipped(t *testing.T) {
	projectID, _, _, project := forgetTestFixtures()

	memID := uuid.New()
	otherNS := uuid.New()

	deleter := newMockMemoryDeleter()
	deleter.memories[memID] = makeMemory(memID, otherNS, nil) // different namespace

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryID:  &memID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 0 {
		t.Fatalf("expected 0 deleted (wrong namespace), got %d", resp.Deleted)
	}
}

// makeSupersededMemory builds a memory whose superseded_by points at parent
// and whose superseded_at is now. Used to seed cascade-walk fixtures.
func makeSupersededMemory(id, namespaceID, parent uuid.UUID, tags []string) *model.Memory {
	mem := makeMemory(id, namespaceID, tags)
	now := time.Now()
	mem.SupersededBy = &parent
	mem.SupersededAt = &now
	return mem
}

func TestForget_ChainTwoDeep_DeletesBoth(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	old := uuid.New()
	head := uuid.New()
	deleter := newMockMemoryDeleter()
	deleter.memories[old] = makeSupersededMemory(old, nsID, head, nil)
	deleter.memories[head] = makeMemory(head, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryID:  &head,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("expected 2 deleted (head + ancestor), got %d", resp.Deleted)
	}
	if !deleter.softDeleted[head] {
		t.Fatal("head should be soft deleted")
	}
	if !deleter.softDeleted[old] {
		t.Fatal("ancestor should be soft deleted via chain cascade")
	}
}

func TestForget_ChainThreeDeep_DeletesAll(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	a := uuid.New()
	b := uuid.New()
	c := uuid.New()

	deleter := newMockMemoryDeleter()
	// A <- B <- C: A is the oldest, C is the active head.
	deleter.memories[a] = makeSupersededMemory(a, nsID, b, nil)
	deleter.memories[b] = makeSupersededMemory(b, nsID, c, nil)
	deleter.memories[c] = makeMemory(c, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryID:  &c,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 3 {
		t.Fatalf("expected 3 deleted (full chain), got %d", resp.Deleted)
	}
	for _, id := range []uuid.UUID{a, b, c} {
		if !deleter.softDeleted[id] {
			t.Fatalf("expected %s to be soft deleted", id)
		}
	}
}

func TestForget_ChainWithCycle_TerminatesViaVisited(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	a := uuid.New()
	b := uuid.New()

	// Pathological: A.SupersededBy = B and B.SupersededBy = A. Cannot occur
	// from production code paths but is the worst case for the cascade walk.
	// The shared visited set must terminate the walk.
	deleter := newMockMemoryDeleter()
	deleter.memories[a] = makeSupersededMemory(a, nsID, b, nil)
	deleter.memories[b] = makeSupersededMemory(b, nsID, a, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryID:  &a,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("expected 2 deleted (cycle handled, no infinite loop), got %d", resp.Deleted)
	}
}

func TestForget_HardDelete_ChainCascadesViaServiceCode(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	old := uuid.New()
	head := uuid.New()
	deleter := newMockMemoryDeleter()
	deleter.memories[old] = makeSupersededMemory(old, nsID, head, nil)
	deleter.memories[head] = makeMemory(head, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID:  projectID,
		MemoryID:   &head,
		HardDelete: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("expected 2 deleted (hard chain cascade), got %d", resp.Deleted)
	}
	// Both should hit the hard delete path: the FK ON DELETE SET NULL
	// would null the pointer at the DB level, but the service-level walk
	// runs first so both rows are accounted for explicitly.
	if !deleter.hardDeleted[head] {
		t.Fatal("head should be hard deleted")
	}
	if !deleter.hardDeleted[old] {
		t.Fatal("ancestor should be hard deleted via service-code cascade")
	}
}

func TestForget_ByTag_HitsAllChainMembersWithTag(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	old := uuid.New()
	head := uuid.New()

	tags := []string{"thread-x"}
	oldMem := makeSupersededMemory(old, nsID, head, tags)
	headMem := makeMemory(head, nsID, tags)

	deleter := newMockMemoryDeleter()
	deleter.memories[old] = oldMem
	deleter.memories[head] = headMem
	// Tag-based forget pages through ListByNamespace; in production a
	// superseded ancestor is excluded from default listings, so a tag
	// filter would only encounter the head. The cascade walk should then
	// pick up the ancestor.
	deleter.nsList = []model.Memory{*headMem}

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		Tags:      tags,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("expected 2 deleted (head listed by tag + ancestor via cascade), got %d", resp.Deleted)
	}
}

func TestForget_ChainDoesNotDoubleCount(t *testing.T) {
	projectID, nsID, _, project := forgetTestFixtures()

	old := uuid.New()
	head := uuid.New()
	deleter := newMockMemoryDeleter()
	deleter.memories[old] = makeSupersededMemory(old, nsID, head, nil)
	deleter.memories[head] = makeMemory(head, nsID, nil)

	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{projectID: project}}
	svc := NewForgetService(deleter, projects, nil, nil)

	// Caller supplies both head and ancestor in MemoryIDs. The visited set
	// must prevent the ancestor from being deleted twice.
	resp, err := svc.Forget(context.Background(), &ForgetRequest{
		ProjectID: projectID,
		MemoryIDs: []uuid.UUID{head, old},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("expected 2 deleted (no double count), got %d", resp.Deleted)
	}
}
