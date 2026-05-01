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
