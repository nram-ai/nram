package service

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// TestMove_RealSQL exercises the full move flow (re-store into destination +
// hard-delete source) against a real SQLite database wired with the real
// StoreService, ForgetService, and repos. It catches composition bugs that the
// mock-based move_test.go cannot:
//
//  1. The source memory is hard-deleted (GetByID returns ErrNoRows after).
//  2. A new memory row exists in the DESTINATION namespace with content, tags,
//     source, and importance preserved.
//  3. The new memory is NOT in the source namespace, and the old one is gone.
//  4. A fresh enrichment job is enqueued for the new memory in the destination.
func TestMove_RealSQL(t *testing.T) {
	// Mirror the update-chain integration test: chdir to a temp dir and open
	// SQLite with default config so a fresh file is created there.
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir tmp: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("migration.NewMigrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("migration Up: %v", err)
	}

	ctx := context.Background()

	nsRepo := storage.NewNamespaceRepo(db)
	projRepo := storage.NewProjectRepo(db)
	memRepo := storage.NewMemoryRepo(db)
	ingestionRepo := storage.NewIngestionLogRepo(db)
	enrichmentRepo := storage.NewEnrichmentQueueRepo(db)

	mkProject := func(name string) *model.Project {
		ns := &model.Namespace{ID: uuid.New(), Name: name, Slug: name, Kind: "user", CreatedAt: time.Now(), UpdatedAt: time.Now()}
		if err := nsRepo.Create(ctx, ns); err != nil {
			t.Fatalf("create namespace %s: %v", name, err)
		}
		proj := &model.Project{
			ID: uuid.New(), NamespaceID: ns.ID, OwnerNamespaceID: ns.ID,
			Name: name, Slug: name, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
		if err := projRepo.Create(ctx, proj); err != nil {
			t.Fatalf("create project %s: %v", name, err)
		}
		return proj
	}

	srcProj := mkProject("move-src")
	dstProj := mkProject("move-dst")

	storeSvc := NewStoreService(memRepo, projRepo, nsRepo, ingestionRepo, enrichmentRepo, nil)
	forgetSvc := NewForgetService(memRepo, projRepo, nil, nil)
	moveSvc := NewMoveService(memRepo, projRepo, storeSvc, forgetSvc)

	// Seed a memory in the source namespace via the repo.
	src := "user-note"
	original := &model.Memory{
		ID: uuid.New(), NamespaceID: srcProj.NamespaceID,
		Content:    "Alice works at Acme",
		Source:     &src,
		Tags:       []string{"person", "employer"},
		Confidence: 1.0, Importance: 0.73,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := memRepo.Create(ctx, original); err != nil {
		t.Fatalf("create original: %v", err)
	}

	// Move it to the destination project.
	resp, err := moveSvc.Move(ctx, &MoveRequest{
		SourceProjectID: srcProj.ID,
		TargetProjectID: dstProj.ID,
		MemoryIDs:       []uuid.UUID{original.ID},
	})
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if resp.Moved != 1 || len(resp.Results) != 1 {
		t.Fatalf("expected 1 moved, got %d (%d results)", resp.Moved, len(resp.Results))
	}
	newID := resp.Results[0].NewID
	if newID == original.ID {
		t.Fatalf("moved memory must get a fresh ID; got original %s", original.ID)
	}

	// 1. Source memory is hard-deleted.
	if _, err := memRepo.GetByID(ctx, original.ID, srcProj.NamespaceID); err == nil {
		t.Errorf("source memory %s should be hard-deleted after move", original.ID)
	}

	// 2 + 3. New memory lives in the DESTINATION namespace, not the source, with
	// fields preserved.
	moved, err := memRepo.GetByID(ctx, newID, dstProj.NamespaceID)
	if err != nil {
		t.Fatalf("reload moved memory: %v", err)
	}
	if moved.NamespaceID != dstProj.NamespaceID {
		t.Errorf("moved memory namespace = %s, want destination %s", moved.NamespaceID, dstProj.NamespaceID)
	}
	if moved.Content != "Alice works at Acme" {
		t.Errorf("content not preserved: %q", moved.Content)
	}
	if moved.Source == nil || *moved.Source != "user-note" {
		t.Errorf("source not preserved: %v", moved.Source)
	}
	if moved.Importance != 0.73 {
		t.Errorf("importance not preserved: %v", moved.Importance)
	}
	if len(moved.Tags) != 2 {
		t.Errorf("tags not preserved: %v", moved.Tags)
	}

	// Source namespace now has zero live memories.
	srcList, err := memRepo.ListByNamespace(ctx, srcProj.NamespaceID, 100, 0)
	if err != nil {
		t.Fatalf("list source: %v", err)
	}
	if len(srcList) != 0 {
		t.Errorf("source namespace should be empty after move; got %d memories", len(srcList))
	}

	// Destination namespace has exactly the moved memory.
	dstList, err := memRepo.ListByNamespace(ctx, dstProj.NamespaceID, 100, 0)
	if err != nil {
		t.Fatalf("list destination: %v", err)
	}
	if len(dstList) != 1 || dstList[0].ID != newID {
		t.Errorf("destination namespace should hold only the moved memory %s; got %d", newID, len(dstList))
	}

	// 4. A fresh enrichment job was enqueued for the new memory in the
	// destination namespace.
	var jobCount int
	if err := db.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM enrichment_queue WHERE memory_id = ? AND namespace_id = ?",
		newID, dstProj.NamespaceID,
	).Scan(&jobCount); err != nil {
		t.Fatalf("count enrichment jobs: %v", err)
	}
	if jobCount != 1 {
		t.Errorf("expected 1 enrichment job for moved memory %s in destination namespace, got %d", newID, jobCount)
	}
}
