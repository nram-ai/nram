package service

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// TestUpdate_ReEmbedUsageRow_KeepsMemoryID pins the write-ordering fix in
// updateSupersede. The re-embed stamps WithMemoryID(newID) on the token_usage
// row; if that row is recorded before the new memory exists, the memory_id
// foreign key fails and TokenUsageRepo's delete-tolerant retry nulls the link.
// Persisting the new memory first must make the row land with the real id.
//
// The bug is FK-driven, so it can only reproduce against a real SQLite DB with
// the real TokenUsageRepo (foreign_keys=ON) — a stub recorder would not fail
// the insert. The embedding provider is the genuine UsageRecordingEmbedding
// middleware wrapping a scripted embedder, backed by the real repo, so the
// re-embed writes a real row through the FK-enforcing INSERT.
func TestUpdate_ReEmbedUsageRow_KeepsMemoryID(t *testing.T) {
	// Mirror TestUpdateAndForgetChain_RealSQL: chdir to a temp dir so SQLite
	// creates a fresh nram.db there.
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
	ns := &model.Namespace{ID: uuid.New(), Name: "reembed", Slug: "reembed", Kind: "user", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := nsRepo.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	projRepo := storage.NewProjectRepo(db)
	proj := &model.Project{
		ID: uuid.New(), NamespaceID: ns.ID, OwnerNamespaceID: ns.ID,
		Name: "reembed", Slug: "reembed",
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := projRepo.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	memRepo := storage.NewMemoryRepo(db)
	enrichmentRepo := storage.NewEnrichmentQueueRepo(db)
	tokenRepo := storage.NewTokenUsageRepo(db)

	const dim = 8
	inner := &mockEmbeddingProvider{
		name:       "scripted",
		dimensions: []int{dim},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, dim)},
			Model:      "scripted-embed",
			Usage:      provider.TokenUsage{PromptTokens: 5},
		},
	}
	recEmbed := provider.NewUsageRecordingEmbedding(inner, tokenRepo, nil)
	updateSvc := NewUpdateService(memRepo, projRepo, nil, func() provider.EmbeddingProvider { return recEmbed }, enrichmentRepo)

	original := &model.Memory{
		ID: uuid.New(), NamespaceID: ns.ID,
		Content:    "The capital of France is Paris",
		Confidence: 1.0, Importance: 0.5,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := memRepo.Create(ctx, original); err != nil {
		t.Fatalf("create original: %v", err)
	}

	newContent := "The capital of Japan is Tokyo"
	resp, err := updateSvc.Update(ctx, &UpdateRequest{
		ProjectID: proj.ID,
		MemoryID:  original.ID,
		Content:   &newContent,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.ID == original.ID {
		t.Fatalf("content update must return a new memory id; got original %s", original.ID)
	}

	// Exactly one embedding row from the re-embed, and its memory_id must be the
	// new (superseding) memory id — not NULL.
	var cnt int
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM token_usage WHERE operation = 'embedding'").Scan(&cnt); err != nil {
		t.Fatalf("count embedding rows: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("expected exactly 1 embedding token_usage row from the re-embed; got %d", cnt)
	}

	var memID sql.NullString
	if err := db.QueryRow(ctx, "SELECT memory_id FROM token_usage WHERE operation = 'embedding'").Scan(&memID); err != nil {
		t.Fatalf("scan memory_id: %v", err)
	}
	if !memID.Valid {
		t.Fatal("re-embed token_usage.memory_id is NULL; the write-ordering bug nulled the FK link")
	}
	if memID.String != resp.ID.String() {
		t.Errorf("re-embed token_usage.memory_id = %s; want new memory id %s", memID.String, resp.ID)
	}
}
