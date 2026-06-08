package admin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

func vmTestVec(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(i%7) * 0.125
	}
	return v
}

// TestVectorMigrationAdminStore_NotConfigured verifies the store returns the
// sentinel (which the handler maps to 400) when no Qdrant address is set,
// before touching the database or attempting a connection.
func TestVectorMigrationAdminStore_NotConfigured(t *testing.T) {
	db := setupAdminTestDB(t)
	store := NewVectorMigrationAdminStore(db, "", storage.DefaultHNSWConfig(),
		func(context.Context) storage.QdrantConfig { return storage.QdrantConfig{Addr: ""} }, nil)

	if _, err := store.DryRun(context.Background(), api.VectorMigrationToQdrant, 0); !errors.Is(err, api.ErrVectorMigrationQdrantNotConfigured) {
		t.Fatalf("DryRun: expected ErrVectorMigrationQdrantNotConfigured, got %v", err)
	}
	if err := store.Start(context.Background(), api.VectorMigrationToQdrant, 0); !errors.Is(err, api.ErrVectorMigrationQdrantNotConfigured) {
		t.Fatalf("Start: expected ErrVectorMigrationQdrantNotConfigured, got %v", err)
	}
}

// TestVectorMigrationAdminStore_ToQdrantLive exercises the full wired glue
// (config fn -> QdrantStore, core, async Start, SSE progress/completed event)
// against a real Qdrant. The source is a fresh local SQLite holding exactly one
// memory vector, so the count is deterministic regardless of what the shared
// Qdrant collection already holds. Non-destructive: random id/namespace,
// deleted on cleanup. Set NRAM_TEST_QDRANT_ADDR to run.
func TestVectorMigrationAdminStore_ToQdrantLive(t *testing.T) {
	addr := os.Getenv("NRAM_TEST_QDRANT_ADDR")
	if addr == "" {
		t.Skip("NRAM_TEST_QDRANT_ADDR not set; skipping live admin-store test")
	}
	ctx := context.Background()
	db := setupAdminTestDB(t)
	nsID := insertTestNamespace(t, db, ctx)

	memID := uuid.New()
	vec := vmTestVec(768)
	execSeed(t, db, ctx, "INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		memID.String(), nsID.String(), "content")
	execSeed(t, db, ctx, "INSERT INTO memory_vectors (memory_id, namespace_id, dimension, embedding) VALUES (?, ?, ?, ?)",
		memID.String(), nsID.String(), 768, hnsw.EncodeVector(vec))

	bus := events.NewEventBus("sqlite", nil, 64, 256)
	defer func() { _ = bus.Close() }()
	cfgFn := func(context.Context) storage.QdrantConfig { return storage.QdrantConfig{Addr: addr} }
	store := NewVectorMigrationAdminStore(db, "", storage.DefaultHNSWConfig(), cfgFn, bus)

	t.Cleanup(func() {
		qs, err := storage.NewQdrantStore(storage.QdrantConfig{Addr: addr})
		if err != nil {
			return
		}
		defer func() { _ = qs.Close() }()
		_ = qs.Delete(context.Background(), storage.VectorKindMemory, memID)
	})

	// Dry run is synchronous and deterministic against the local source.
	dry, err := store.DryRun(ctx, api.VectorMigrationToQdrant, 100)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if !dry.DryRun || dry.MemoryCount != 1 {
		t.Fatalf("dry run: dry=%v memory=%d (want true/1)", dry.DryRun, dry.MemoryCount)
	}

	// Subscribe before Start so we catch the terminal event.
	ch, cancel, err := bus.Subscribe(ctx, events.EventScopeVectorMigration)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	if err := store.Start(ctx, api.VectorMigrationToQdrant, 100); err != nil {
		t.Fatalf("start: %v", err)
	}

	sawProgress := false
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for vector_migration.completed event")
		case evt := <-ch:
			switch evt.Type {
			case events.VectorMigrationProgress:
				sawProgress = true
			case events.VectorMigrationFailed:
				t.Fatalf("migration failed event: %s", string(evt.Data))
			case events.VectorMigrationCompleted:
				if !sawProgress {
					t.Error("no vector_migration.progress event observed before completion")
				}
				var res api.VectorMigrationResult
				if err := json.Unmarshal(evt.Data, &res); err != nil {
					t.Fatalf("decode completed event: %v", err)
				}
				if res.MemoryCount != 1 {
					t.Fatalf("completed event memory_count=%d, want 1", res.MemoryCount)
				}
				// Verify the point landed in Qdrant.
				qs, err := storage.NewQdrantStore(storage.QdrantConfig{Addr: addr})
				if err != nil {
					t.Fatalf("verify connect: %v", err)
				}
				defer func() { _ = qs.Close() }()
				got, err := qs.GetByIDs(ctx, storage.VectorKindMemory, []uuid.UUID{memID}, 768)
				if err != nil {
					t.Fatalf("verify get: %v", err)
				}
				if _, ok := got[memID]; !ok {
					t.Fatalf("migrated memory %s not found in qdrant", memID)
				}
				return
			}
		}
	}
}
