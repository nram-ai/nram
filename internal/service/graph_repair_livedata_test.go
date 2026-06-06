package service

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/storage"
)

// TestRepairGraph_LiveData runs the actual production RepairGraph path against a
// restored copy of a real database, proving the repair on real data. It is
// skipped unless REPRO_DATABASE_URL points at a throwaway restore (NEVER a live
// system: RepairGraph permanently deletes lost-provenance rows). Usage:
//
//	REPRO_DATABASE_URL='postgres://user@127.0.0.1:5432/nram_liverepro?sslmode=disable' \
//	  go test ./internal/service/ -run TestRepairGraph_LiveData -count=1 -v
func TestRepairGraph_LiveData(t *testing.T) {
	url := os.Getenv("REPRO_DATABASE_URL")
	if url == "" {
		t.Skip("REPRO_DATABASE_URL not set; skipping live-data repair verification")
	}

	db, err := storage.Open(config.DatabaseConfig{URL: url})
	if err != nil {
		t.Fatalf("open repro db: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	erepo := storage.NewEntityRepo(db)
	rrepo := storage.NewRelationshipRepo(db)
	reaper := NewGraphReaper(rrepo, erepo)
	pruner := NewGraphPruner(erepo, rrepo)
	// Pin OrphanGrace so the orphan sweep does not depend on a SettingsService;
	// 1h protects any genuinely in-flight enrichment in the restored snapshot.
	svc := NewLifecycleService(nil, nil, pruner, LifecycleConfig{OrphanGrace: time.Hour}, nil).
		WithGraphReaper(reaper)

	before, err := rrepo.CountLostProvenance(ctx)
	if err != nil {
		t.Fatalf("count before: %v", err)
	}
	t.Logf("BEFORE: %d lost-provenance relationships", before)

	res, err := svc.RepairGraph(ctx)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	t.Logf("REPAIR: reaped=%d relationships, pruned dangling=%d, orphaned entities=%d",
		res.RelationshipsReaped, res.DanglingDeleted, res.OrphanedEntities)

	after, err := rrepo.CountLostProvenance(ctx)
	if err != nil {
		t.Fatalf("count after: %v", err)
	}
	t.Logf("AFTER: %d lost-provenance relationships", after)

	if after != 0 {
		t.Fatalf("expected 0 lost-provenance edges after repair, got %d", after)
	}
	if res.RelationshipsReaped != before {
		t.Fatalf("reaped %d but %d were present before", res.RelationshipsReaped, before)
	}

	// Idempotent: a second repair reaps nothing.
	res2, err := svc.RepairGraph(ctx)
	if err != nil {
		t.Fatalf("second repair: %v", err)
	}
	if res2.RelationshipsReaped != 0 {
		t.Fatalf("second repair reaped %d, expected 0 (not idempotent)", res2.RelationshipsReaped)
	}
}
