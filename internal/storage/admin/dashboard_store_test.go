package admin

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage"
)

// seedAliceUserUnderOrg creates an org, an "alice" user namespace and user
// row under it, and a project namespace under alice. Returns alice's user ID
// and the project namespace ID so callers can attach memories. Backend-aware
// via execSeed.
func seedAliceUserUnderOrg(t *testing.T, db storage.DB, ctx context.Context) (orgID, aliceID, aliceProjNsID uuid.UUID) {
	t.Helper()
	orgID, orgNsID := insertOrgWithNamespace(t, db, ctx)

	aliceNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		aliceNsID.String(), "alice", "alice", "user", "test-org/alice", 1, orgNsID.String())

	aliceID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO users (id, email, org_id, namespace_id) VALUES (?, ?, ?, ?)",
		aliceID.String(), "alice@test", orgID.String(), aliceNsID.String())

	aliceProjNsID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		aliceProjNsID.String(), "alice-proj", "alice-proj", "project", "test-org/alice/alice-proj", 2, aliceNsID.String())
	return orgID, aliceID, aliceProjNsID
}

// insertMemoryRaw inserts a memory with arbitrary content bytes (which may
// be invalid UTF-8). The Postgres test database is created with SQL_ASCII
// encoding (see setupAdminTestPostgres) so any byte sequence is admitted
// in TEXT columns, simulating production where bad bytes already exist.
// SQLite admits arbitrary bytes by default.
func insertMemoryRaw(t *testing.T, db storage.DB, ctx context.Context, nsID uuid.UUID, content []byte, accessCount int) uuid.UUID {
	t.Helper()
	memID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content, access_count) VALUES (?, ?, ?, ?)",
		memID.String(), nsID.String(), string(content), accessCount)
	return memID
}

// seedSecondUserWithProject seeds a second user (bob) under the same org
// with their own project namespace. Returns bob's user ID, project
// namespace ID, and project ID so dashboard tests can verify that bob's
// project names do NOT leak into alice's self-tier per-project breakdown.
func seedSecondUserWithProject(t *testing.T, db storage.DB, ctx context.Context, orgID uuid.UUID) (bobID, bobProjNsID, bobProjID uuid.UUID) {
	t.Helper()
	var orgNsID string
	row := db.QueryRow(ctx, "SELECT namespace_id FROM organizations WHERE id = ?", orgID.String())
	if db.Backend() == storage.BackendPostgres {
		row = db.QueryRow(ctx, "SELECT namespace_id FROM organizations WHERE id = $1", orgID.String())
	}
	if err := row.Scan(&orgNsID); err != nil {
		t.Fatalf("look up org namespace: %v", err)
	}

	bobNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		bobNsID.String(), "bob", "bob", "user", "test-org/bob", 1, orgNsID)

	bobID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO users (id, email, org_id, namespace_id) VALUES (?, ?, ?, ?)",
		bobID.String(), "bob@test", orgID.String(), bobNsID.String())

	bobProjNsID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		bobProjNsID.String(), "bob-secret-project", "bob-secret-project", "project", "test-org/bob/bob-secret-project", 2, bobNsID.String())

	bobProjID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		bobProjID.String(), "bob-secret-project", "bob-secret-project", bobProjNsID.String(), bobNsID.String())
	return
}

// TestDashboardStoreStats_PerProjectScopedToCaller asserts the self-tier
// dashboard's per-project breakdown returns only the caller's own
// projects, with names populated. A second user in the same org has a
// distinctively-named project that must NOT appear in the caller's
// response. The regression this test guards against is the 2026-05-25
// cross-user project-name leak that prompted the dashboard fix.
//
// When orgID is set but userID is nil (the legacy call shape), names
// must be omitted from the response so an org-aggregate consumer does
// not learn cross-user names. The third case (both nil, global) is
// covered implicitly by the existing global counts path; not re-tested
// here because the production handler always passes orgID for an
// authenticated caller.
func TestDashboardStoreStats_PerProjectScopedToCaller(t *testing.T) {
	for _, bk := range adminTestBackends {
		t.Run(bk.name, func(t *testing.T) {
			db := bk.setup(t)
			queueRepo := storage.NewEnrichmentQueueRepo(db)
			store := NewDashboardStore(db, queueRepo)
			ctx := context.Background()

			orgID, aliceID, aliceProjNsID := seedAliceUserUnderOrg(t, db, ctx)
			// Insert the projects row for alice's project (the seed
			// helper above only creates the namespace).
			aliceProjID := uuid.New()
			execSeed(t, db, ctx,
				"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
				aliceProjID.String(), "alice-proj", "alice-proj", aliceProjNsID.String(),
				// owner_namespace_id is the user namespace; look it up via the project ns parent.
				lookupParentNamespaceID(t, db, ctx, aliceProjNsID).String())
			insertMemoryRaw(t, db, ctx, aliceProjNsID, []byte("alice memory"), 1)

			_, bobProjNsID, bobProjID := seedSecondUserWithProject(t, db, ctx, orgID)
			insertMemoryRaw(t, db, ctx, bobProjNsID, []byte("bob memory"), 1)

			// Self-tier (orgID + userID): only alice's project, with name.
			stats, err := store.DashboardStats(ctx, &orgID, &aliceID)
			if err != nil {
				t.Fatalf("DashboardStats(self-tier): %v", err)
			}
			if len(stats.MemoriesByProject) != 1 {
				t.Fatalf("self-tier: expected 1 project, got %d (%+v)", len(stats.MemoriesByProject), stats.MemoriesByProject)
			}
			row := stats.MemoriesByProject[0]
			if row.ProjectID != aliceProjID {
				if row.ProjectID == bobProjID {
					t.Errorf("self-tier leaked bob's project %s", bobProjID)
				} else {
					t.Errorf("self-tier ProjectID: got %s want %s", row.ProjectID, aliceProjID)
				}
			}
			if row.ProjectName != "alice-proj" {
				t.Errorf("self-tier ProjectName: got %q want %q", row.ProjectName, "alice-proj")
			}

			// Org-aggregate (orgID set, userID nil): both projects, but
			// names omitted to prevent the cross-user leak.
			statsOrg, err := store.DashboardStats(ctx, &orgID, nil)
			if err != nil {
				t.Fatalf("DashboardStats(org-aggregate): %v", err)
			}
			if len(statsOrg.MemoriesByProject) != 2 {
				t.Fatalf("org-aggregate: expected 2 projects, got %d (%+v)", len(statsOrg.MemoriesByProject), statsOrg.MemoriesByProject)
			}
			for i, r := range statsOrg.MemoriesByProject {
				if r.ProjectName != "" {
					t.Errorf("org-aggregate row[%d].ProjectName: got %q, want empty (org tier emits UUID only)", i, r.ProjectName)
				}
			}
		})
	}
}

// lookupParentNamespaceID resolves the parent_id of a namespace by ID,
// returning it as a uuid.UUID. Used by tests that need the user
// namespace owning a project, given only the project namespace ID.
func lookupParentNamespaceID(t *testing.T, db storage.DB, ctx context.Context, nsID uuid.UUID) uuid.UUID {
	t.Helper()
	var parentStr string
	row := db.QueryRow(ctx, "SELECT parent_id FROM namespaces WHERE id = ?", nsID.String())
	if db.Backend() == storage.BackendPostgres {
		row = db.QueryRow(ctx, "SELECT parent_id FROM namespaces WHERE id = $1", nsID.String())
	}
	if err := row.Scan(&parentStr); err != nil {
		t.Fatalf("look up parent namespace: %v", err)
	}
	parent, err := uuid.Parse(parentStr)
	if err != nil {
		t.Fatalf("parse parent namespace id: %v", err)
	}
	return parent
}

// TestDashboardStoreRecentActivity_UserScoped exercises the self-tier path
// with the production-shape call (both orgID and userID non-nil), which is
// what SelfScope produces for any authenticated caller. Runs on both
// SQLite and Postgres so the backend-specific SQL stays in sync.
func TestDashboardStoreRecentActivity_UserScoped(t *testing.T) {
	for _, bk := range adminTestBackends {
		t.Run(bk.name, func(t *testing.T) {
			db := bk.setup(t)
			store := NewDashboardStore(db, nil)
			ctx := context.Background()

			orgID, aliceID, aliceProjNsID := seedAliceUserUnderOrg(t, db, ctx)
			insertMemoryRaw(t, db, ctx, aliceProjNsID, []byte("alice memory"), 1)

			events, err := store.RecentActivity(ctx, 20, &orgID, &aliceID)
			if err != nil {
				t.Fatalf("RecentActivity user-scoped returned error: %v", err)
			}
			if len(events) != 1 {
				t.Fatalf("expected 1 event, got %d", len(events))
			}
			if events[0].LengthChars != len("alice memory") {
				t.Errorf("expected length %d, got %d", len("alice memory"), events[0].LengthChars)
			}
			if events[0].Preview == nil {
				t.Fatal("expected non-nil preview on self-tier")
			}
			if *events[0].Preview != "alice memory" {
				t.Errorf("expected preview %q, got %q", "alice memory", *events[0].Preview)
			}
		})
	}
}

// TestDashboardStoreRecentActivity_UserScoped_HostileContent reproduces both
// production failure modes that surfaced on dev:
//
//   - SQLSTATE 22021 from LENGTH/SUBSTRING on text whose bytes are invalid
//     UTF-8 (e.g. a lone 0xe2 lead byte).
//   - SQLSTATE 22P02 from m.content::bytea on text containing a backslash
//     followed by something that isn't a valid bytea escape (e.g. C:\path,
//     a JSON string literal, anything with `\` adjacent to a non-octal,
//     non-`x`, non-`\` byte).
//
// The fix is to select content raw and slice the preview in Go after
// running strings.ToValidUTF8. Both rows must come back with valid-UTF-8
// previews and the request must succeed.
func TestDashboardStoreRecentActivity_UserScoped_HostileContent(t *testing.T) {
	for _, bk := range adminTestBackends {
		t.Run(bk.name, func(t *testing.T) {
			db := bk.setup(t)
			store := NewDashboardStore(db, nil)
			ctx := context.Background()

			orgID, aliceID, aliceProjNsID := seedAliceUserUnderOrg(t, db, ctx)

			invalidUTF8 := append([]byte{0xe2}, []byte("bad")...)
			insertMemoryRaw(t, db, ctx, aliceProjNsID, invalidUTF8, 1)
			windowsPath := []byte(`C:\path\to\file`)
			insertMemoryRaw(t, db, ctx, aliceProjNsID, windowsPath, 1)

			events, err := store.RecentActivity(ctx, 20, &orgID, &aliceID)
			if err != nil {
				t.Fatalf("RecentActivity user-scoped with hostile content returned error: %v", err)
			}
			if len(events) != 2 {
				t.Fatalf("expected 2 events, got %d", len(events))
			}
			for i, ev := range events {
				if ev.Preview == nil {
					t.Fatalf("event %d: expected non-nil preview", i)
				}
				if !utf8.ValidString(*ev.Preview) {
					t.Errorf("event %d: preview is not valid UTF-8: %q", i, *ev.Preview)
				}
			}
		})
	}
}
