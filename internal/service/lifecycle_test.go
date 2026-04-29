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

// --- Lifecycle mock implementations ---

type mockLifecycleStore struct {
	expired     []model.Memory
	purgeable   []model.Memory
	softDeleted map[uuid.UUID]bool
	hardDeleted map[uuid.UUID]bool
	expiredErr  error
	purgeErr    error
	softErr     error
	hardErr     error
}

func newMockLifecycleStore() *mockLifecycleStore {
	return &mockLifecycleStore{
		softDeleted: make(map[uuid.UUID]bool),
		hardDeleted: make(map[uuid.UUID]bool),
	}
}

func (m *mockLifecycleStore) ListExpired(_ context.Context, _ time.Time, limit int) ([]model.Memory, error) {
	if m.expiredErr != nil {
		return nil, m.expiredErr
	}
	if limit > len(m.expired) {
		limit = len(m.expired)
	}
	return m.expired[:limit], nil
}

func (m *mockLifecycleStore) ListPurgeable(_ context.Context, _ time.Time, limit int) ([]model.Memory, error) {
	if m.purgeErr != nil {
		return nil, m.purgeErr
	}
	if limit > len(m.purgeable) {
		limit = len(m.purgeable)
	}
	return m.purgeable[:limit], nil
}

func (m *mockLifecycleStore) SoftDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	if m.softErr != nil {
		return m.softErr
	}
	m.softDeleted[id] = true
	return nil
}

func (m *mockLifecycleStore) HardDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	if m.hardErr != nil {
		return m.hardErr
	}
	m.hardDeleted[id] = true
	return nil
}

type mockLifecycleVectorDeleter struct {
	deleted map[uuid.UUID]bool
}

func newMockLifecycleVectorDeleter() *mockLifecycleVectorDeleter {
	return &mockLifecycleVectorDeleter{deleted: make(map[uuid.UUID]bool)}
}

func (m *mockLifecycleVectorDeleter) Delete(_ context.Context, _ storage.VectorKind, id uuid.UUID) error {
	m.deleted[id] = true
	return nil
}

// --- Helpers ---

func makeExpiredMemory(id uuid.UUID) model.Memory {
	past := time.Now().Add(-1 * time.Hour)
	return model.Memory{
		ID:         id,
		Content:    "expired content",
		Confidence: 1.0,
		Importance: 0.5,
		ExpiresAt:  &past,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
}

func makePurgeableMemory(id uuid.UUID) model.Memory {
	past := time.Now().Add(-1 * time.Hour)
	deleted := time.Now().Add(-48 * time.Hour)
	return model.Memory{
		ID:         id,
		Content:    "purgeable content",
		Confidence: 1.0,
		Importance: 0.5,
		DeletedAt:  &deleted,
		PurgeAfter: &past,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
}

// --- Tests ---

func TestSweep_ExpiresMemoriesPastTTL(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()

	store := newMockLifecycleStore()
	store.expired = []model.Memory{makeExpiredMemory(id1), makeExpiredMemory(id2)}

	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{}, nil)

	expired, purged, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expired != 2 {
		t.Fatalf("expected 2 expired, got %d", expired)
	}
	if purged != 0 {
		t.Fatalf("expected 0 purged, got %d", purged)
	}
	if !store.softDeleted[id1] || !store.softDeleted[id2] {
		t.Fatal("expected both memories to be soft deleted")
	}
}

func TestSweep_PurgesMemoriesPastPurgeAfter(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()

	store := newMockLifecycleStore()
	store.purgeable = []model.Memory{makePurgeableMemory(id1), makePurgeableMemory(id2)}

	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{}, nil)

	expired, purged, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expired != 0 {
		t.Fatalf("expected 0 expired, got %d", expired)
	}
	if purged != 2 {
		t.Fatalf("expected 2 purged, got %d", purged)
	}
	if !store.hardDeleted[id1] || !store.hardDeleted[id2] {
		t.Fatal("expected both memories to be hard deleted")
	}
}

func TestSweep_NoExpiredOrPurgeable(t *testing.T) {
	store := newMockLifecycleStore()
	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{}, nil)

	expired, purged, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expired != 0 || purged != 0 {
		t.Fatalf("expected (0, 0), got (%d, %d)", expired, purged)
	}
}

func TestSweep_VectorStoreCleanupOnPurge(t *testing.T) {
	id1 := uuid.New()

	store := newMockLifecycleStore()
	store.purgeable = []model.Memory{makePurgeableMemory(id1)}

	vectors := newMockLifecycleVectorDeleter()
	svc := NewLifecycleService(store, vectors, nil, LifecycleConfig{}, nil)

	_, purged, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if purged != 1 {
		t.Fatalf("expected 1 purged, got %d", purged)
	}
	if !vectors.deleted[id1] {
		t.Fatal("expected memory to be deleted from vector store")
	}
}

func TestSweep_NilVectorStoreNoPanic(t *testing.T) {
	id1 := uuid.New()

	store := newMockLifecycleStore()
	store.purgeable = []model.Memory{makePurgeableMemory(id1)}

	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{}, nil)

	_, purged, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if purged != 1 {
		t.Fatalf("expected 1 purged, got %d", purged)
	}
	if !store.hardDeleted[id1] {
		t.Fatal("expected memory to be hard deleted")
	}
}

func TestStartStop_NoPanicOrHang(t *testing.T) {
	store := newMockLifecycleStore()
	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{
		SweepInterval: 50 * time.Millisecond,
	}, nil)

	svc.Start()

	// Let it tick at least once.
	time.Sleep(100 * time.Millisecond)

	// Stop should return without hanging.
	done := make(chan struct{})
	go func() {
		svc.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2 seconds")
	}
}

func TestConfigDefaults(t *testing.T) {
	store := newMockLifecycleStore()
	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{}, nil)

	// SweepInterval is read once at construction from settings (or its default
	// when settings is nil).
	if svc.config.SweepInterval != 5*time.Minute {
		t.Fatalf("expected default SweepInterval 5m, got %v", svc.config.SweepInterval)
	}
	// BatchSize, DefaultPurgeDelay, OrphanGrace are now resolved per-sweep.
	// With config zero and settings nil, the resolver helpers fall through
	// to settingDefaults.
	if got := svc.resolveBatchSize(context.Background()); got != 100 {
		t.Fatalf("expected resolveBatchSize 100, got %d", got)
	}
	if got := svc.resolvePurgeDelay(context.Background()); got != 30*24*time.Hour {
		t.Fatalf("expected resolvePurgeDelay 30d, got %v", got)
	}
}

func TestConfigCustom(t *testing.T) {
	store := newMockLifecycleStore()
	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{
		SweepInterval:     10 * time.Minute,
		BatchSize:         50,
		DefaultPurgeDelay: 7 * 24 * time.Hour,
	}, nil)

	if svc.config.SweepInterval != 10*time.Minute {
		t.Fatalf("expected SweepInterval 10m, got %v", svc.config.SweepInterval)
	}
	// Operator-pinned values short-circuit the per-sweep resolver.
	if got := svc.resolveBatchSize(context.Background()); got != 50 {
		t.Fatalf("expected resolveBatchSize 50, got %d", got)
	}
	if got := svc.resolvePurgeDelay(context.Background()); got != 7*24*time.Hour {
		t.Fatalf("expected resolvePurgeDelay 7d, got %v", got)
	}
}

func TestSweep_ListExpiredError(t *testing.T) {
	store := newMockLifecycleStore()
	store.expiredErr = fmt.Errorf("db connection failed")

	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{}, nil)

	_, _, err := svc.Sweep(context.Background())
	if err == nil {
		t.Fatal("expected error from ListExpired failure")
	}
}

func TestSweep_ListPurgeableError(t *testing.T) {
	store := newMockLifecycleStore()
	store.purgeErr = fmt.Errorf("db connection failed")

	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{}, nil)

	expired, _, err := svc.Sweep(context.Background())
	if err == nil {
		t.Fatal("expected error from ListPurgeable failure")
	}
	// Expired phase should still have run (0 expired since none listed).
	if expired != 0 {
		t.Fatalf("expected 0 expired, got %d", expired)
	}
}

func TestSweep_SoftDeleteErrorSkipsMemory(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()

	store := newMockLifecycleStore()
	store.expired = []model.Memory{makeExpiredMemory(id1), makeExpiredMemory(id2)}
	store.softErr = fmt.Errorf("soft delete failed")

	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{}, nil)

	expired, _, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expired != 0 {
		t.Fatalf("expected 0 expired (all failed), got %d", expired)
	}
}

func TestSweep_HardDeleteErrorSkipsMemory(t *testing.T) {
	id1 := uuid.New()

	store := newMockLifecycleStore()
	store.purgeable = []model.Memory{makePurgeableMemory(id1)}
	store.hardErr = fmt.Errorf("hard delete failed")

	vectors := newMockLifecycleVectorDeleter()
	svc := NewLifecycleService(store, vectors, nil, LifecycleConfig{}, nil)

	_, purged, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if purged != 0 {
		t.Fatalf("expected 0 purged (hard delete failed), got %d", purged)
	}
	// Vector should NOT be deleted if hard delete failed.
	if vectors.deleted[id1] {
		t.Fatal("vector should not be deleted when hard delete fails")
	}
}

func TestSweep_BatchSizeRespected(t *testing.T) {
	store := newMockLifecycleStore()
	// Add 5 expired memories but set batch size to 3.
	for i := 0; i < 5; i++ {
		store.expired = append(store.expired, makeExpiredMemory(uuid.New()))
	}

	svc := NewLifecycleService(store, nil, nil, LifecycleConfig{
		BatchSize: 3,
	}, nil)

	expired, _, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expired != 3 {
		t.Fatalf("expected 3 expired (batch limited), got %d", expired)
	}
}

func TestSweep_BothExpiredAndPurgeable(t *testing.T) {
	expID := uuid.New()
	purgeID := uuid.New()

	store := newMockLifecycleStore()
	store.expired = []model.Memory{makeExpiredMemory(expID)}
	store.purgeable = []model.Memory{makePurgeableMemory(purgeID)}

	vectors := newMockLifecycleVectorDeleter()
	svc := NewLifecycleService(store, vectors, nil, LifecycleConfig{}, nil)

	expired, purged, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected 1 expired, got %d", expired)
	}
	if purged != 1 {
		t.Fatalf("expected 1 purged, got %d", purged)
	}
	if !store.softDeleted[expID] {
		t.Fatal("expected expired memory to be soft deleted")
	}
	if !store.hardDeleted[purgeID] {
		t.Fatal("expected purgeable memory to be hard deleted")
	}
	if !vectors.deleted[purgeID] {
		t.Fatal("expected purgeable memory to be removed from vector store")
	}
}
