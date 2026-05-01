package admin

import (
	"context"
	"strings"
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

// TestDashboardStoreRecentActivity_UserScoped_InvalidUTF8 reproduces the
// production failure: a memory whose content carries an invalid UTF-8 byte
// (lone 0xe2 lead) caused LENGTH/SUBSTRING to raise SQLSTATE 22021 on
// Postgres. Byte-level SQL plus strings.ToValidUTF8 must keep the query
// from erroring and yield a valid-UTF-8 preview with U+FFFD in place of
// the invalid byte.
func TestDashboardStoreRecentActivity_UserScoped_InvalidUTF8(t *testing.T) {
	for _, bk := range adminTestBackends {
		t.Run(bk.name, func(t *testing.T) {
			db := bk.setup(t)
			store := NewDashboardStore(db, nil)
			ctx := context.Background()

			orgID, aliceID, aliceProjNsID := seedAliceUserUnderOrg(t, db, ctx)

			// Lone 0xe2 lead byte followed by ASCII — invalid UTF-8.
			badContent := append([]byte{0xe2}, []byte("bad")...)
			insertMemoryRaw(t, db, ctx, aliceProjNsID, badContent, 1)

			events, err := store.RecentActivity(ctx, 20, &orgID, &aliceID)
			if err != nil {
				t.Fatalf("RecentActivity user-scoped with invalid UTF-8 returned error: %v", err)
			}
			if len(events) != 1 {
				t.Fatalf("expected 1 event, got %d", len(events))
			}
			if events[0].Preview == nil {
				t.Fatal("expected non-nil preview")
			}
			if !utf8.ValidString(*events[0].Preview) {
				t.Errorf("preview is not valid UTF-8: %q", *events[0].Preview)
			}
			if !strings.Contains(*events[0].Preview, "�") {
				t.Errorf("expected U+FFFD replacement char in preview, got %q", *events[0].Preview)
			}
		})
	}
}
