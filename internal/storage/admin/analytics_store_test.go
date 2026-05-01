package admin

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage"
)

// insertOrgWithNamespace creates a namespace and organization for testing.
func insertOrgWithNamespace(t *testing.T, db storage.DB, ctx context.Context) (orgID, nsID uuid.UUID) {
	t.Helper()
	nsID = insertTestNamespace(t, db, ctx)
	orgID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO organizations (id, name, slug, namespace_id) VALUES (?, ?, ?, ?)",
		orgID.String(), "Test Org", "test-org", nsID.String())
	return orgID, nsID
}

func TestAnalyticsStoreGetAnalytics_GlobalNoData(t *testing.T) {
	db := setupAdminTestDB(t)
	store := NewAnalyticsStore(db)

	data, err := store.GetAnalytics(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("GetAnalytics global returned error: %v", err)
	}
	if data.MemoryCounts.Total != 0 {
		t.Errorf("expected 0 total memories, got %d", data.MemoryCounts.Total)
	}
}

func TestAnalyticsStoreGetAnalytics_OrgScopedNoData(t *testing.T) {
	db := setupAdminTestDB(t)
	store := NewAnalyticsStore(db)
	ctx := context.Background()

	orgID, _ := insertOrgWithNamespace(t, db, ctx)

	data, err := store.GetAnalytics(ctx, &orgID, nil)
	if err != nil {
		t.Fatalf("GetAnalytics org-scoped returned error: %v", err)
	}
	if data.MemoryCounts.Total != 0 {
		t.Errorf("expected 0 total memories, got %d", data.MemoryCounts.Total)
	}
}

func TestAnalyticsStoreGetAnalytics_OrgScopedWithMemories(t *testing.T) {
	db := setupAdminTestDB(t)
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

	data, err := store.GetAnalytics(ctx, &orgID, nil)
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
	db := setupAdminTestDB(t)
	store := NewAnalyticsStore(db)
	ctx := context.Background()

	// Use a random org ID that doesn't exist in the database.
	fakeOrgID := uuid.New()

	data, err := store.GetAnalytics(ctx, &fakeOrgID, nil)
	if err != nil {
		t.Fatalf("GetAnalytics with nonexistent org returned error: %v", err)
	}
	if data.MemoryCounts.Total != 0 {
		t.Errorf("expected 0 total memories, got %d", data.MemoryCounts.Total)
	}
}

// TestAnalyticsStoreGetAnalytics_UserScoped seeds two users in the same org
// with one memory each and confirms a user-scoped query returns only that
// user's memory, not the teammate's.
func TestAnalyticsStoreGetAnalytics_UserScoped(t *testing.T) {
	db := setupAdminTestDB(t)
	store := NewAnalyticsStore(db)
	ctx := context.Background()

	orgID, orgNsID := insertOrgWithNamespace(t, db, ctx)

	// Two user namespaces under the org.
	aliceNsID := uuid.New()
	if _, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		aliceNsID.String(), "alice", "alice", "user", "test-org/alice", 1, orgNsID.String()); err != nil {
		t.Fatalf("insert alice namespace: %v", err)
	}
	bobNsID := uuid.New()
	if _, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		bobNsID.String(), "bob", "bob", "user", "test-org/bob", 1, orgNsID.String()); err != nil {
		t.Fatalf("insert bob namespace: %v", err)
	}

	aliceID := uuid.New()
	if _, err := db.Exec(ctx,
		"INSERT INTO users (id, email, org_id, namespace_id) VALUES (?, ?, ?, ?)",
		aliceID.String(), "alice@test", orgID.String(), aliceNsID.String()); err != nil {
		t.Fatalf("insert alice: %v", err)
	}
	bobID := uuid.New()
	if _, err := db.Exec(ctx,
		"INSERT INTO users (id, email, org_id, namespace_id) VALUES (?, ?, ?, ?)",
		bobID.String(), "bob@test", orgID.String(), bobNsID.String()); err != nil {
		t.Fatalf("insert bob: %v", err)
	}

	// One project namespace per user.
	aliceProjNsID := uuid.New()
	if _, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		aliceProjNsID.String(), "alice-proj", "alice-proj", "project", "test-org/alice/alice-proj", 2, aliceNsID.String()); err != nil {
		t.Fatalf("insert alice project namespace: %v", err)
	}
	bobProjNsID := uuid.New()
	if _, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		bobProjNsID.String(), "bob-proj", "bob-proj", "project", "test-org/bob/bob-proj", 2, bobNsID.String()); err != nil {
		t.Fatalf("insert bob project namespace: %v", err)
	}

	// One memory per user.
	if _, err := db.Exec(ctx,
		"INSERT INTO memories (id, namespace_id, content, access_count) VALUES (?, ?, ?, ?)",
		uuid.New().String(), aliceProjNsID.String(), "alice memory", 7); err != nil {
		t.Fatalf("insert alice memory: %v", err)
	}
	if _, err := db.Exec(ctx,
		"INSERT INTO memories (id, namespace_id, content, access_count) VALUES (?, ?, ?, ?)",
		uuid.New().String(), bobProjNsID.String(), "bob memory", 3); err != nil {
		t.Fatalf("insert bob memory: %v", err)
	}

	// Production-shape self-tier call: SelfScope returns both pointers
	// non-nil, and the store dispatches to the userID branch.
	data, err := store.GetAnalytics(ctx, &orgID, &aliceID)
	if err != nil {
		t.Fatalf("GetAnalytics user-scoped returned error: %v", err)
	}
	if data.MemoryCounts.Total != 1 {
		t.Errorf("expected 1 total memory for alice, got %d", data.MemoryCounts.Total)
	}
	if len(data.MostRecalled) != 1 {
		t.Fatalf("expected 1 most recalled, got %d", len(data.MostRecalled))
	}
	if data.MostRecalled[0].LengthChars != len("alice memory") {
		t.Errorf("expected length %d for alice memory, got %d", len("alice memory"), data.MostRecalled[0].LengthChars)
	}

	// Org-scoped sees both.
	orgData, err := store.GetAnalytics(ctx, &orgID, nil)
	if err != nil {
		t.Fatalf("GetAnalytics org-scoped returned error: %v", err)
	}
	if orgData.MemoryCounts.Total != 2 {
		t.Errorf("expected 2 org memories, got %d", orgData.MemoryCounts.Total)
	}

	// Global sees both.
	globalData, err := store.GetAnalytics(ctx, nil, nil)
	if err != nil {
		t.Fatalf("GetAnalytics global returned error: %v", err)
	}
	if globalData.MemoryCounts.Total != 2 {
		t.Errorf("expected 2 global memories, got %d", globalData.MemoryCounts.Total)
	}
}

// TestAnalyticsStoreGetAnalytics_UserScoped_HostileContent covers both
// production failure modes: invalid UTF-8 (raises 22021 under text-aware
// LENGTH/SUBSTRING) and backslash sequences that don't form a valid bytea
// escape (raise 22P02 under m.content::bytea). The fix is to select content
// raw and slice the preview in Go.
func TestAnalyticsStoreGetAnalytics_UserScoped_HostileContent(t *testing.T) {
	for _, bk := range adminTestBackends {
		t.Run(bk.name, func(t *testing.T) {
			db := bk.setup(t)
			store := NewAnalyticsStore(db)
			ctx := context.Background()

			orgID, aliceID, aliceProjNsID := seedAliceUserUnderOrg(t, db, ctx)
			invalidUTF8 := append([]byte{0xe2}, []byte("bad")...)
			insertMemoryRaw(t, db, ctx, aliceProjNsID, invalidUTF8, 5)
			windowsPath := []byte(`C:\path\to\file`)
			insertMemoryRaw(t, db, ctx, aliceProjNsID, windowsPath, 7)

			data, err := store.GetAnalytics(ctx, &orgID, &aliceID)
			if err != nil {
				t.Fatalf("GetAnalytics user-scoped with hostile content returned error: %v", err)
			}
			if data.MemoryCounts.Total != 2 {
				t.Errorf("expected 2 total memories, got %d", data.MemoryCounts.Total)
			}
			if len(data.MostRecalled) != 2 {
				t.Fatalf("expected 2 most recalled, got %d", len(data.MostRecalled))
			}
			for i, item := range data.MostRecalled {
				if item.Preview == nil {
					t.Fatalf("item %d: expected non-nil preview", i)
				}
				if !utf8.ValidString(*item.Preview) {
					t.Errorf("item %d: preview is not valid UTF-8: %q", i, *item.Preview)
				}
			}
		})
	}
}
