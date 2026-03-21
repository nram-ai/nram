package admin

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/storage"
)

func setupAnalyticsTestDB(t *testing.T) storage.DB {
	t.Helper()
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("create migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return db
}

// insertOrgWithNamespace creates a namespace and organization for testing.
func insertOrgWithNamespace(t *testing.T, db storage.DB, ctx context.Context) (orgID, nsID uuid.UUID) {
	t.Helper()
	orgID = uuid.New()
	nsID = uuid.New()
	_, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth) VALUES (?, ?, ?, ?, ?, ?)",
		nsID.String(), "test-org", "test-org", "org", "test-org", 0)
	if err != nil {
		t.Fatalf("insert namespace: %v", err)
	}
	_, err = db.Exec(ctx,
		"INSERT INTO organizations (id, name, slug, namespace_id) VALUES (?, ?, ?, ?)",
		orgID.String(), "Test Org", "test-org", nsID.String())
	if err != nil {
		t.Fatalf("insert org: %v", err)
	}
	return orgID, nsID
}

func TestAnalyticsStoreGetAnalytics_GlobalNoData(t *testing.T) {
	db := setupAnalyticsTestDB(t)
	store := NewAnalyticsStore(db)

	data, err := store.GetAnalytics(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetAnalytics global returned error: %v", err)
	}
	if data.MemoryCounts.Total != 0 {
		t.Errorf("expected 0 total memories, got %d", data.MemoryCounts.Total)
	}
}

func TestAnalyticsStoreGetAnalytics_OrgScopedNoData(t *testing.T) {
	db := setupAnalyticsTestDB(t)
	store := NewAnalyticsStore(db)
	ctx := context.Background()

	orgID, _ := insertOrgWithNamespace(t, db, ctx)

	data, err := store.GetAnalytics(ctx, &orgID)
	if err != nil {
		t.Fatalf("GetAnalytics org-scoped returned error: %v", err)
	}
	if data.MemoryCounts.Total != 0 {
		t.Errorf("expected 0 total memories, got %d", data.MemoryCounts.Total)
	}
}

func TestAnalyticsStoreGetAnalytics_OrgScopedWithMemories(t *testing.T) {
	db := setupAnalyticsTestDB(t)
	store := NewAnalyticsStore(db)
	ctx := context.Background()

	orgID, orgNsID := insertOrgWithNamespace(t, db, ctx)

	// Create project namespace (child of org).
	projNsID := uuid.New()
	_, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), "my-project", "my-project", "project", "test-org/my-project", 1, orgNsID.String())
	if err != nil {
		t.Fatalf("insert project namespace: %v", err)
	}

	// Create a memory in the project namespace.
	memID := uuid.New()
	_, err = db.Exec(ctx,
		`INSERT INTO memories (id, namespace_id, content, access_count) VALUES (?, ?, ?, ?)`,
		memID.String(), projNsID.String(), "test memory content", 5)
	if err != nil {
		t.Fatalf("insert memory: %v", err)
	}

	data, err := store.GetAnalytics(ctx, &orgID)
	if err != nil {
		t.Fatalf("GetAnalytics org-scoped with memories returned error: %v", err)
	}
	if data.MemoryCounts.Total != 1 {
		t.Errorf("expected 1 total memory, got %d", data.MemoryCounts.Total)
	}
	if data.MemoryCounts.Active != 1 {
		t.Errorf("expected 1 active memory, got %d", data.MemoryCounts.Active)
	}
	if len(data.MostRecalled) != 1 {
		t.Errorf("expected 1 most recalled, got %d", len(data.MostRecalled))
	}
}

func TestAnalyticsStoreGetAnalytics_OrgScopedNoOrg(t *testing.T) {
	db := setupAnalyticsTestDB(t)
	store := NewAnalyticsStore(db)
	ctx := context.Background()

	// Use a random org ID that doesn't exist in the database.
	fakeOrgID := uuid.New()

	data, err := store.GetAnalytics(ctx, &fakeOrgID)
	if err != nil {
		t.Fatalf("GetAnalytics with nonexistent org returned error: %v", err)
	}
	if data.MemoryCounts.Total != 0 {
		t.Errorf("expected 0 total memories, got %d", data.MemoryCounts.Total)
	}
}
