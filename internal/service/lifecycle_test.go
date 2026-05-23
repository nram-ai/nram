package service

import (
	"context"
	"fmt"
	"os"
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
	if got := svc.resolveBatchSize(context.Background()); got != 1000 {
		t.Fatalf("expected resolveBatchSize 1000, got %d", got)
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

// kindAwareVectorDeleter records both the VectorKind and ID of every Delete
// call so orphan-sweep tests can assert entity vectors specifically (the
// memory-side mockLifecycleVectorDeleter discards the kind).
type kindAwareVectorDeleter struct {
	calls []recordedKindDelete
}

type recordedKindDelete struct {
	kind storage.VectorKind
	id   uuid.UUID
}

func (m *kindAwareVectorDeleter) Delete(_ context.Context, kind storage.VectorKind, id uuid.UUID) error {
	m.calls = append(m.calls, recordedKindDelete{kind: kind, id: id})
	return nil
}

// TestSweep_OrphanedEntityCascadeAndVectorCleanup wires a real EntityRepo +
// GraphPruner against a freshly migrated SQLite DB so the FK-cascade
// migration (000035) is exercised end-to-end. Pre-fix, the orphan branch
// raised SQLSTATE 23503 on entity_aliases_entity_id_fkey and the sweep
// silently logged-and-continued, leaving entities and the vector store
// permanently out of sync.
func TestSweep_OrphanedEntityCascadeAndVectorCleanup(t *testing.T) {
	ctx := context.Background()
	fx := seedProject(t)

	// Pre-existing entity with an alias, backdated past the orphan grace.
	entID := uuid.New()
	ent := &model.Entity{
		ID: entID, NamespaceID: fx.target.NamespaceID,
		Name: "orphan_e2e", Canonical: "orphan_e2e", EntityType: "thing",
	}
	if err := fx.entityRepo.Create(ctx, ent); err != nil {
		t.Fatalf("create entity: %v", err)
	}
	aliasID := uuid.New()
	if _, err := fx.db.Exec(ctx,
		`INSERT INTO entity_aliases (id, namespace_id, entity_id, alias, alias_type) VALUES (?, ?, ?, ?, ?)`,
		aliasID.String(), fx.target.NamespaceID.String(), entID.String(), "OE", "ticker",
	); err != nil {
		t.Fatalf("insert alias: %v", err)
	}
	if _, err := fx.db.Exec(ctx,
		`UPDATE entities SET created_at = ? WHERE id = ?`,
		"2020-01-01T00:00:00Z", entID.String(),
	); err != nil {
		t.Fatalf("backdate created_at: %v", err)
	}

	store := newMockLifecycleStore() // memory paths inert for this test
	vec := &kindAwareVectorDeleter{}
	pruner := NewGraphPruner(fx.entityRepo, storage.NewRelationshipRepo(fx.db))
	svc := NewLifecycleService(store, vec, pruner, LifecycleConfig{
		OrphanGrace: time.Hour, // entity is from 2020, far older than 1h
	}, nil)

	if _, _, err := svc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if n := countRows(t, fx.db, `SELECT COUNT(*) FROM entities WHERE id = ?`, entID.String()); n != 0 {
		t.Errorf("expected entity gone after sweep, found %d rows", n)
	}
	if n := countRows(t, fx.db, `SELECT COUNT(*) FROM entity_aliases WHERE id = ?`, aliasID.String()); n != 0 {
		t.Errorf("expected alias cascaded out, found %d rows (FK CASCADE migration not applied?)", n)
	}

	var entityVectorDeletes int
	for _, c := range vec.calls {
		if c.kind == storage.VectorKindEntity && c.id == entID {
			entityVectorDeletes++
		}
	}
	if entityVectorDeletes != 1 {
		t.Errorf("expected 1 vector store Delete(entity, %s), got %d (calls=%v)",
			entID, entityVectorDeletes, vec.calls)
	}
}

// TestSweep_OrphanedEntityVectorCleanup_Qdrant runs the same sweep against a
// real Qdrant instance to prove the cleanup call actually removes the point.
// Pre-fix, the SQL CASCADE on entity_vectors_* would have handled SQLite /
// pgvector deployments (no leak), but Qdrant deployments would silently
// accumulate dead points keyed by deleted entity UUIDs — that's the leak this
// test pins.
func TestSweep_OrphanedEntityVectorCleanup_Qdrant(t *testing.T) {
	addr := os.Getenv("QDRANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set QDRANT_TEST_ADDR to run Qdrant integration sweep test")
	}

	ctx := context.Background()
	fx := seedProject(t)

	qstore, err := storage.NewQdrantStore(storage.QdrantConfig{Addr: addr})
	if err != nil {
		t.Fatalf("NewQdrantStore: %v", err)
	}
	t.Cleanup(func() { qstore.Close() })
	if err := qstore.EnsureCollections(ctx); err != nil {
		t.Fatalf("EnsureCollections: %v", err)
	}

	entID := uuid.New()
	ent := &model.Entity{
		ID: entID, NamespaceID: fx.target.NamespaceID,
		Name: "orphan_qdrant", Canonical: "orphan_qdrant_" + uuid.NewString()[:8], EntityType: "thing",
	}
	if err := fx.entityRepo.Create(ctx, ent); err != nil {
		t.Fatalf("create entity: %v", err)
	}
	if _, err := fx.db.Exec(ctx,
		`UPDATE entities SET created_at = ? WHERE id = ?`,
		"2020-01-01T00:00:00Z", entID.String(),
	); err != nil {
		t.Fatalf("backdate created_at: %v", err)
	}

	dim := 384
	emb := make([]float32, dim)
	emb[0] = 1.0
	for i := 1; i < dim; i++ {
		emb[i] = 0.01
	}
	if err := qstore.Upsert(ctx, storage.VectorKindEntity, entID, fx.target.NamespaceID, emb, dim); err != nil {
		t.Fatalf("upsert vector: %v", err)
	}
	t.Cleanup(func() {
		// Belt-and-suspenders cleanup if the test fails before the sweep.
		_ = qstore.Delete(context.Background(), storage.VectorKindEntity, entID)
	})

	got, err := qstore.GetByIDs(ctx, storage.VectorKindEntity, []uuid.UUID{entID}, dim)
	if err != nil {
		t.Fatalf("GetByIDs pre-sweep: %v", err)
	}
	if _, present := got[entID]; !present {
		t.Fatalf("vector not present pre-sweep")
	}

	store := newMockLifecycleStore()
	pruner := NewGraphPruner(fx.entityRepo, storage.NewRelationshipRepo(fx.db))
	svc := NewLifecycleService(store, qstore, pruner, LifecycleConfig{
		OrphanGrace: time.Hour,
	}, nil)
	if _, _, err := svc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	got, err = qstore.GetByIDs(ctx, storage.VectorKindEntity, []uuid.UUID{entID}, dim)
	if err != nil {
		t.Fatalf("GetByIDs post-sweep: %v", err)
	}
	if _, present := got[entID]; present {
		t.Fatalf("vector %s still present in Qdrant after orphan sweep (got=%v)", entID, got)
	}
}
