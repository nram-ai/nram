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

// TestUpdateAndForgetChain_RealSQL exercises the full content-update +
// chain-forget flow against a real SQLite database to catch composition
// bugs that mocks would miss. It verifies, end-to-end through real SQL:
//
//  1. memory_update with content change creates a new memory row and
//     marks the old one superseded via SupersedeReplacing's transaction.
//  2. A second content update on the new head produces a 3-deep chain.
//  3. ListByNamespaceFiltered with HideSuperseded surfaces only the
//     active head; without it surfaces the full chain.
//  4. memory_forget on the active head walks SupersededBy via the new
//     FindBySupersededBy and soft-deletes the entire chain.
//  5. After the forget, every chain row has deleted_at set; default
//     reads return ErrNoRows / 404 semantics.
//
// This is the test that pins composition: the service's update path,
// the storage's transactional helper, the cascade walk in forget, and
// the schema's HideSuperseded filter all agree on the same semantics.
func TestUpdateAndForgetChain_RealSQL(t *testing.T) {
	// Mirror internal/storage's testSQLiteDB: chdir to a temp dir and
	// open with default config so SQLite creates a fresh file there.
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir tmp: %v", err)
	}
	t.Cleanup(func() { os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("migration.NewMigrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("migration Up: %v", err)
	}

	ctx := context.Background()

	// Seed namespace + project so memory FK constraints are satisfied.
	nsRepo := storage.NewNamespaceRepo(db)
	ns := &model.Namespace{ID: uuid.New(), Name: "chain-test", Slug: "chain-test", Kind: "user", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := nsRepo.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	projRepo := storage.NewProjectRepo(db)
	proj := &model.Project{
		ID: uuid.New(), NamespaceID: ns.ID, OwnerNamespaceID: ns.ID,
		Name: "chain-test", Slug: "chain-test",
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := projRepo.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	memRepo := storage.NewMemoryRepo(db)
	enrichmentRepo := storage.NewEnrichmentQueueRepo(db)
	forgetSvc := NewForgetService(memRepo, projRepo, nil, nil)
	updateSvc := NewUpdateService(memRepo, projRepo, nil, nil, enrichmentRepo)

	// 1. Seed an initial memory directly via the repo (the StoreService
	//    has many other dependencies; this test focuses on the update +
	//    forget composition, not the store path which is well-tested
	//    elsewhere).
	original := &model.Memory{
		ID: uuid.New(), NamespaceID: ns.ID,
		Content:    "Alice works at Acme",
		Tags:       []string{"person", "employer"},
		Confidence: 1.0, Importance: 0.5,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := memRepo.Create(ctx, original); err != nil {
		t.Fatalf("create original: %v", err)
	}

	// 2. First content update → A → B chain.
	newContentB := "Alice works at Beta Corp"
	respB, err := updateSvc.Update(ctx, &UpdateRequest{
		ProjectID: proj.ID,
		MemoryID:  original.ID,
		Content:   &newContentB,
	})
	if err != nil {
		t.Fatalf("first update: %v", err)
	}
	if respB.ID == original.ID {
		t.Fatalf("first update must return a new ID; got original %s", original.ID)
	}
	if !respB.Superseded {
		t.Errorf("respB.Superseded should be true on content change")
	}

	// 3. Second content update → A → B → C chain.
	newContentC := "Alice works at Gamma LLC"
	respC, err := updateSvc.Update(ctx, &UpdateRequest{
		ProjectID: proj.ID,
		MemoryID:  respB.ID,
		Content:   &newContentC,
	})
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
	if respC.ID == respB.ID || respC.ID == original.ID {
		t.Fatalf("second update must return a third ID; got %s (B=%s, A=%s)", respC.ID, respB.ID, original.ID)
	}

	// 4. Chain shape via direct row reads.
	gotA, err := memRepo.GetByID(ctx, original.ID)
	if err != nil {
		t.Fatalf("reload A: %v", err)
	}
	if gotA.SupersededBy == nil || *gotA.SupersededBy != respB.ID {
		t.Errorf("A.SupersededBy = %v, want %s", gotA.SupersededBy, respB.ID)
	}
	gotB, err := memRepo.GetByID(ctx, respB.ID)
	if err != nil {
		t.Fatalf("reload B: %v", err)
	}
	if gotB.SupersededBy == nil || *gotB.SupersededBy != respC.ID {
		t.Errorf("B.SupersededBy = %v, want %s", gotB.SupersededBy, respC.ID)
	}
	gotC, err := memRepo.GetByID(ctx, respC.ID)
	if err != nil {
		t.Fatalf("reload C: %v", err)
	}
	if gotC.SupersededBy != nil {
		t.Errorf("C.SupersededBy should be nil (active head); got %v", gotC.SupersededBy)
	}

	// 5. Default list filters via HideSuperseded → only C surfaces.
	visible, err := memRepo.ListByNamespaceFiltered(ctx, ns.ID,
		storage.MemoryListFilters{HideSuperseded: true}, 100, 0)
	if err != nil {
		t.Fatalf("list hide superseded: %v", err)
	}
	if len(visible) != 1 || visible[0].ID != respC.ID {
		ids := make([]string, len(visible))
		for i, m := range visible {
			ids[i] = m.ID.String()
		}
		t.Errorf("HideSuperseded should return only the active head %s; got %v", respC.ID, ids)
	}

	// Without HideSuperseded → all three rows surface.
	all, err := memRepo.ListByNamespaceFiltered(ctx, ns.ID,
		storage.MemoryListFilters{}, 100, 0)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 rows visible without HideSuperseded; got %d", len(all))
	}

	// 6. Forget the active head → cascade walks the chain.
	forgetResp, err := forgetSvc.Forget(ctx, &ForgetRequest{
		ProjectID: proj.ID,
		MemoryID:  &respC.ID,
	})
	if err != nil {
		t.Fatalf("forget head: %v", err)
	}
	if forgetResp.Deleted != 3 {
		t.Errorf("forget should soft-delete the entire chain (3 rows); got %d", forgetResp.Deleted)
	}

	// 7. After the cascade every chain row should be soft-deleted; reads
	//    by ID should return ErrNoRows since GetByID filters deleted_at.
	for _, id := range []uuid.UUID{original.ID, respB.ID, respC.ID} {
		if _, err := memRepo.GetByID(ctx, id); err == nil {
			t.Errorf("memory %s should be soft-deleted after chain forget", id)
		}
	}
}
