package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- Mock implementations for batch get tests ---

type mockBatchMemoryReader struct {
	memories map[uuid.UUID]*model.Memory
	batchErr error
}

func (m *mockBatchMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mem, nil
}

func (m *mockBatchMemoryReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	if m.batchErr != nil {
		return nil, m.batchErr
	}
	var result []model.Memory
	for _, id := range ids {
		if mem, ok := m.memories[id]; ok {
			result = append(result, *mem)
		}
	}
	return result, nil
}

func (m *mockBatchMemoryReader) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return nil, nil
}

type mockBatchProjectRepo struct {
	projects map[uuid.UUID]*model.Project
}

func (m *mockBatchProjectRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	p, ok := m.projects[id]
	if !ok {
		return nil, fmt.Errorf("project not found")
	}
	return p, nil
}

// --- Test helpers ---

func newTestMemory(id, namespaceID uuid.UUID, deleted bool) *model.Memory {
	mem := &model.Memory{
		ID:          id,
		NamespaceID: namespaceID,
		Content:     "test content for " + id.String(),
		Tags:        []string{"tag1"},
		Enriched:    true,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if deleted {
		now := time.Now()
		mem.DeletedAt = &now
	}
	return mem
}

func TestBatchGet_AllFound(t *testing.T) {
	nsID := uuid.New()
	projID := uuid.New()
	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()

	reader := &mockBatchMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: newTestMemory(id1, nsID, false),
			id2: newTestMemory(id2, nsID, false),
			id3: newTestMemory(id3, nsID, false),
		},
	}
	projects := &mockBatchProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projID: {ID: projID, NamespaceID: nsID},
		},
	}

	svc := NewBatchGetService(reader, projects)
	resp, err := svc.BatchGet(context.Background(), &BatchGetRequest{
		ProjectID: projID,
		IDs:       []uuid.UUID{id1, id2, id3},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Found) != 3 {
		t.Errorf("expected 3 found, got %d", len(resp.Found))
	}
	if len(resp.NotFound) != 0 {
		t.Errorf("expected 0 not_found, got %d", len(resp.NotFound))
	}
}

func TestBatchGet_SomeFoundSomeNotFound(t *testing.T) {
	nsID := uuid.New()
	projID := uuid.New()
	id1, id2, idMissing := uuid.New(), uuid.New(), uuid.New()

	reader := &mockBatchMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: newTestMemory(id1, nsID, false),
			id2: newTestMemory(id2, nsID, false),
		},
	}
	projects := &mockBatchProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projID: {ID: projID, NamespaceID: nsID},
		},
	}

	svc := NewBatchGetService(reader, projects)
	resp, err := svc.BatchGet(context.Background(), &BatchGetRequest{
		ProjectID: projID,
		IDs:       []uuid.UUID{id1, idMissing, id2},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Found) != 2 {
		t.Errorf("expected 2 found, got %d", len(resp.Found))
	}
	if len(resp.NotFound) != 1 {
		t.Errorf("expected 1 not_found, got %d", len(resp.NotFound))
	}
	if resp.NotFound[0] != idMissing {
		t.Errorf("expected not_found to contain %s, got %s", idMissing, resp.NotFound[0])
	}
}

func TestBatchGet_AllNotFound(t *testing.T) {
	nsID := uuid.New()
	projID := uuid.New()
	id1, id2 := uuid.New(), uuid.New()

	reader := &mockBatchMemoryReader{
		memories: map[uuid.UUID]*model.Memory{},
	}
	projects := &mockBatchProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projID: {ID: projID, NamespaceID: nsID},
		},
	}

	svc := NewBatchGetService(reader, projects)
	resp, err := svc.BatchGet(context.Background(), &BatchGetRequest{
		ProjectID: projID,
		IDs:       []uuid.UUID{id1, id2},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Found) != 0 {
		t.Errorf("expected 0 found, got %d", len(resp.Found))
	}
	if len(resp.NotFound) != 2 {
		t.Errorf("expected 2 not_found, got %d", len(resp.NotFound))
	}
}

func TestBatchGet_EmptyIDsError(t *testing.T) {
	projID := uuid.New()
	nsID := uuid.New()

	reader := &mockBatchMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	projects := &mockBatchProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projID: {ID: projID, NamespaceID: nsID},
		},
	}

	svc := NewBatchGetService(reader, projects)
	_, err := svc.BatchGet(context.Background(), &BatchGetRequest{
		ProjectID: projID,
		IDs:       []uuid.UUID{},
	})
	if err == nil {
		t.Fatal("expected error for empty IDs, got nil")
	}
}

func TestBatchGet_ProjectNotFoundError(t *testing.T) {
	reader := &mockBatchMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	projects := &mockBatchProjectRepo{
		projects: map[uuid.UUID]*model.Project{},
	}

	svc := NewBatchGetService(reader, projects)
	_, err := svc.BatchGet(context.Background(), &BatchGetRequest{
		ProjectID: uuid.New(),
		IDs:       []uuid.UUID{uuid.New()},
	})
	if err == nil {
		t.Fatal("expected error for missing project, got nil")
	}
}

func TestBatchGet_WrongNamespaceExcluded(t *testing.T) {
	nsID := uuid.New()
	otherNsID := uuid.New()
	projID := uuid.New()
	idCorrect := uuid.New()
	idWrongNs := uuid.New()

	reader := &mockBatchMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			idCorrect: newTestMemory(idCorrect, nsID, false),
			idWrongNs: newTestMemory(idWrongNs, otherNsID, false),
		},
	}
	projects := &mockBatchProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projID: {ID: projID, NamespaceID: nsID},
		},
	}

	svc := NewBatchGetService(reader, projects)
	resp, err := svc.BatchGet(context.Background(), &BatchGetRequest{
		ProjectID: projID,
		IDs:       []uuid.UUID{idCorrect, idWrongNs},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Found) != 1 {
		t.Errorf("expected 1 found, got %d", len(resp.Found))
	}
	if resp.Found[0].ID != idCorrect {
		t.Errorf("expected found memory %s, got %s", idCorrect, resp.Found[0].ID)
	}
	if len(resp.NotFound) != 1 {
		t.Errorf("expected 1 not_found, got %d", len(resp.NotFound))
	}
	if resp.NotFound[0] != idWrongNs {
		t.Errorf("expected not_found to contain %s, got %s", idWrongNs, resp.NotFound[0])
	}
}

func TestBatchGet_SoftDeletedExcluded(t *testing.T) {
	nsID := uuid.New()
	projID := uuid.New()
	idActive := uuid.New()
	idDeleted := uuid.New()

	reader := &mockBatchMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			idActive:  newTestMemory(idActive, nsID, false),
			idDeleted: newTestMemory(idDeleted, nsID, true),
		},
	}
	projects := &mockBatchProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projID: {ID: projID, NamespaceID: nsID},
		},
	}

	svc := NewBatchGetService(reader, projects)
	resp, err := svc.BatchGet(context.Background(), &BatchGetRequest{
		ProjectID: projID,
		IDs:       []uuid.UUID{idActive, idDeleted},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Found) != 1 {
		t.Errorf("expected 1 found, got %d", len(resp.Found))
	}
	if resp.Found[0].ID != idActive {
		t.Errorf("expected found memory %s, got %s", idActive, resp.Found[0].ID)
	}
	if len(resp.NotFound) != 1 {
		t.Errorf("expected 1 not_found, got %d", len(resp.NotFound))
	}
	if resp.NotFound[0] != idDeleted {
		t.Errorf("expected not_found to contain %s, got %s", idDeleted, resp.NotFound[0])
	}
}

func TestBatchGet_LatencyTracked(t *testing.T) {
	nsID := uuid.New()
	projID := uuid.New()
	id1 := uuid.New()

	reader := &mockBatchMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			id1: newTestMemory(id1, nsID, false),
		},
	}
	projects := &mockBatchProjectRepo{
		projects: map[uuid.UUID]*model.Project{
			projID: {ID: projID, NamespaceID: nsID},
		},
	}

	svc := NewBatchGetService(reader, projects)
	resp, err := svc.BatchGet(context.Background(), &BatchGetRequest{
		ProjectID: projID,
		IDs:       []uuid.UUID{id1},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}
