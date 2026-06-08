package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// --- Move mocks ---

type fakeMoveMemReader struct {
	memories map[uuid.UUID]*model.Memory
}

func (f *fakeMoveMemReader) GetByID(_ context.Context, id uuid.UUID, namespaceID uuid.UUID) (*model.Memory, error) {
	m, ok := f.memories[id]
	if !ok || m.NamespaceID != namespaceID {
		return nil, fmt.Errorf("not found")
	}
	return m, nil
}

type fakeMoveStore struct {
	calls   []*StoreRequest
	nextID  uuid.UUID
	dedupID uuid.UUID // if set, returned instead of a fresh id
	err     error
}

func (f *fakeMoveStore) Store(_ context.Context, req *StoreRequest) (*StoreResponse, error) {
	f.calls = append(f.calls, req)
	if f.err != nil {
		return nil, f.err
	}
	id := f.nextID
	if f.dedupID != uuid.Nil {
		id = f.dedupID
	}
	if id == uuid.Nil {
		id = uuid.New()
	}
	return &StoreResponse{ID: id, ProjectID: req.ProjectID, Content: req.Content, Tags: req.Tags}, nil
}

type fakeMoveForget struct {
	calls []*ForgetRequest
	err   error
}

func (f *fakeMoveForget) Forget(_ context.Context, req *ForgetRequest) (*ForgetResponse, error) {
	f.calls = append(f.calls, req)
	if f.err != nil {
		return nil, f.err
	}
	return &ForgetResponse{Deleted: 1}, nil
}

func moveTestSetup(srcNS uuid.UUID) (uuid.UUID, uuid.UUID, *mockForgetProjectRepo) {
	srcProjectID := uuid.New()
	dstProjectID := uuid.New()
	dstNS := uuid.New()
	projects := &mockForgetProjectRepo{projects: map[uuid.UUID]*model.Project{
		srcProjectID: {ID: srcProjectID, NamespaceID: srcNS, Slug: "src"},
		dstProjectID: {ID: dstProjectID, NamespaceID: dstNS, Slug: "dst"},
	}}
	return srcProjectID, dstProjectID, projects
}

// --- Tests ---

func TestMove_SingleHappyPath(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, dstProjectID, projects := moveTestSetup(srcNS)
	memID := uuid.New()
	src := "user-note"
	reader := &fakeMoveMemReader{memories: map[uuid.UUID]*model.Memory{
		memID: {ID: memID, NamespaceID: srcNS, Content: "hello", Source: &src, Tags: []string{"a"}, Importance: 0.7},
	}}
	store := &fakeMoveStore{nextID: uuid.New()}
	forget := &fakeMoveForget{}

	svc := NewMoveService(reader, projects, store, forget)
	resp, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: dstProjectID,
		MemoryIDs:       []uuid.UUID{memID},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Moved != 1 || len(resp.Results) != 1 {
		t.Fatalf("expected 1 moved, got %d (%d results)", resp.Moved, len(resp.Results))
	}
	if resp.Results[0].OldID != memID || resp.Results[0].NewID != store.nextID {
		t.Fatalf("result ids wrong: %+v", resp.Results[0])
	}
	if len(store.calls) != 1 {
		t.Fatalf("expected 1 store call, got %d", len(store.calls))
	}
	sc := store.calls[0]
	if sc.ProjectID != dstProjectID {
		t.Errorf("store targeted wrong project: %v", sc.ProjectID)
	}
	if sc.Content != "hello" || sc.Source != "user-note" {
		t.Errorf("store content/source not preserved: %q / %q", sc.Content, sc.Source)
	}
	if sc.Importance == nil || *sc.Importance != 0.7 {
		t.Errorf("importance not preserved: %v", sc.Importance)
	}
	if len(forget.calls) != 1 {
		t.Fatalf("expected 1 forget call, got %d", len(forget.calls))
	}
	fc := forget.calls[0]
	if fc.ProjectID != srcProjectID || !fc.HardDelete || fc.MemoryID == nil || *fc.MemoryID != memID {
		t.Errorf("forget call wrong: %+v", fc)
	}
}

func TestMove_BulkHappyPath(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, dstProjectID, projects := moveTestSetup(srcNS)
	id1, id2 := uuid.New(), uuid.New()
	reader := &fakeMoveMemReader{memories: map[uuid.UUID]*model.Memory{
		id1: {ID: id1, NamespaceID: srcNS, Content: "one", Importance: 0.5},
		id2: {ID: id2, NamespaceID: srcNS, Content: "two", Importance: 0.5},
	}}
	store := &fakeMoveStore{}
	forget := &fakeMoveForget{}

	svc := NewMoveService(reader, projects, store, forget)
	resp, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: dstProjectID,
		MemoryIDs:       []uuid.UUID{id1, id2},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Moved != 2 {
		t.Fatalf("expected 2 moved, got %d", resp.Moved)
	}
	if len(store.calls) != 2 || len(forget.calls) != 2 {
		t.Fatalf("expected 2 store + 2 forget, got %d / %d", len(store.calls), len(forget.calls))
	}
}

func TestMove_SameProjectRejected(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, _, projects := moveTestSetup(srcNS)
	svc := NewMoveService(&fakeMoveMemReader{}, projects, &fakeMoveStore{}, &fakeMoveForget{})
	_, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: srcProjectID,
		MemoryIDs:       []uuid.UUID{uuid.New()},
	})
	if err == nil {
		t.Fatal("expected error for same source/target project")
	}
}

func TestMove_MemoryInDifferentNamespaceSkipped(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, dstProjectID, projects := moveTestSetup(srcNS)
	memID := uuid.New()
	// Memory lives in a foreign namespace, not the source project's.
	reader := &fakeMoveMemReader{memories: map[uuid.UUID]*model.Memory{
		memID: {ID: memID, NamespaceID: uuid.New(), Content: "foreign"},
	}}
	store := &fakeMoveStore{}
	forget := &fakeMoveForget{}

	svc := NewMoveService(reader, projects, store, forget)
	resp, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: dstProjectID,
		MemoryIDs:       []uuid.UUID{memID},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Moved != 0 {
		t.Errorf("expected 0 moved, got %d", resp.Moved)
	}
	if len(store.calls) != 0 || len(forget.calls) != 0 {
		t.Errorf("store/forget must not be called for foreign memory: %d / %d", len(store.calls), len(forget.calls))
	}
}

func TestMove_StoreFailsLeavesSourceIntact(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, dstProjectID, projects := moveTestSetup(srcNS)
	memID := uuid.New()
	reader := &fakeMoveMemReader{memories: map[uuid.UUID]*model.Memory{
		memID: {ID: memID, NamespaceID: srcNS, Content: "x"},
	}}
	store := &fakeMoveStore{err: fmt.Errorf("embed provider down")}
	forget := &fakeMoveForget{}

	svc := NewMoveService(reader, projects, store, forget)
	resp, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: dstProjectID,
		MemoryIDs:       []uuid.UUID{memID},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Moved != 0 {
		t.Errorf("expected 0 moved when store fails, got %d", resp.Moved)
	}
	if len(forget.calls) != 0 {
		t.Errorf("source must NOT be deleted when store fails (data loss guard); got %d forget calls", len(forget.calls))
	}
}

func TestMove_ReservedSourceDropped(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, dstProjectID, projects := moveTestSetup(srcNS)
	memID := uuid.New()
	dream := model.DreamSource
	reader := &fakeMoveMemReader{memories: map[uuid.UUID]*model.Memory{
		memID: {ID: memID, NamespaceID: srcNS, Content: "insight", Source: &dream},
	}}
	store := &fakeMoveStore{}
	forget := &fakeMoveForget{}

	svc := NewMoveService(reader, projects, store, forget)
	if _, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: dstProjectID,
		MemoryIDs:       []uuid.UUID{memID},
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("expected 1 store call, got %d", len(store.calls))
	}
	if store.calls[0].Source != "" {
		t.Errorf("reserved source must be dropped, got %q", store.calls[0].Source)
	}
}

func TestMove_ForgetFailsStillCountsAsMoved(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, dstProjectID, projects := moveTestSetup(srcNS)
	memID := uuid.New()
	reader := &fakeMoveMemReader{memories: map[uuid.UUID]*model.Memory{
		memID: {ID: memID, NamespaceID: srcNS, Content: "x"},
	}}
	store := &fakeMoveStore{nextID: uuid.New()}
	forget := &fakeMoveForget{err: fmt.Errorf("delete blew up")}

	svc := NewMoveService(reader, projects, store, forget)
	resp, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: dstProjectID,
		MemoryIDs:       []uuid.UUID{memID},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Destination copy is valid even though source cleanup failed.
	if resp.Moved != 1 || len(resp.Results) != 1 {
		t.Fatalf("expected the destination copy to count as moved, got %d", resp.Moved)
	}
}

func TestMove_MissingMemorySkipped(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, dstProjectID, projects := moveTestSetup(srcNS)
	store := &fakeMoveStore{}
	forget := &fakeMoveForget{}
	svc := NewMoveService(&fakeMoveMemReader{memories: map[uuid.UUID]*model.Memory{}}, projects, store, forget)
	resp, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: dstProjectID,
		MemoryIDs:       []uuid.UUID{uuid.New()},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Moved != 0 {
		t.Errorf("expected 0 moved for missing memory, got %d", resp.Moved)
	}
}

func TestMove_EmptyIDsRejected(t *testing.T) {
	srcNS := uuid.New()
	srcProjectID, dstProjectID, projects := moveTestSetup(srcNS)
	svc := NewMoveService(&fakeMoveMemReader{}, projects, &fakeMoveStore{}, &fakeMoveForget{})
	if _, err := svc.Move(context.Background(), &MoveRequest{
		SourceProjectID: srcProjectID,
		TargetProjectID: dstProjectID,
		MemoryIDs:       nil,
	}); err == nil {
		t.Fatal("expected error for empty memory ids")
	}
}
