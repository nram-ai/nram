package admin

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

func TestClampAuditQueryLimit(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"zero defaults to 100", 0, defaultAuditQueryLimit},
		{"negative defaults to 100", -1, defaultAuditQueryLimit},
		{"large negative defaults to 100", math.MinInt, defaultAuditQueryLimit},
		{"one is honored", 1, 1},
		{"small positive is honored", 5, 5},
		{"value at cap is honored", maxAuditQueryLimit, maxAuditQueryLimit},
		{"cap+1 is clamped to cap", maxAuditQueryLimit + 1, maxAuditQueryLimit},
		{"MaxInt is clamped to cap", math.MaxInt, maxAuditQueryLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := clampAuditQueryLimit(tc.in)
			if got != tc.want {
				t.Errorf("clampAuditQueryLimit(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// seedAuditEvents inserts n audit events stamped at distinct (descending)
// times so ORDER BY occurred_at DESC has stable ordering.
func seedAuditEvents(t *testing.T, store *AuditStore, ctx context.Context, n int) {
	t.Helper()
	base := time.Now().UTC()
	for i := range n {
		ev := api.AuditEvent{
			Action:     "test.event",
			OccurredAt: base.Add(time.Duration(-i) * time.Millisecond),
		}
		if err := store.Append(ctx, ev); err != nil {
			t.Fatalf("seed Append[%d]: %v", i, err)
		}
	}
}

// TestAuditStoreQuery_FloorAppliedAtDB confirms that the floor wired into
// the SQL LIMIT actually constrains rows when more rows than the floor
// exist in the table. This is the integration anchor for the helper.
func TestAuditStoreQuery_FloorAppliedAtDB(t *testing.T) {
	db := setupAdminTestDB(t)
	store := NewAuditStore(db)
	ctx := context.Background()

	seedAuditEvents(t, store, ctx, defaultAuditQueryLimit+50)

	got, err := store.Query(ctx, api.AuditScope{}, time.Time{}, 0)
	if err != nil {
		t.Fatalf("Query(limit=0) returned error: %v", err)
	}
	if len(got) != defaultAuditQueryLimit {
		t.Errorf("limit=0 should default to %d rows, got %d", defaultAuditQueryLimit, len(got))
	}
}

// TestAuditStoreQuery_HonorsExplicitSmallLimit confirms the SQL LIMIT path
// is wired to the (clamped) value, not ignored.
func TestAuditStoreQuery_HonorsExplicitSmallLimit(t *testing.T) {
	db := setupAdminTestDB(t)
	store := NewAuditStore(db)
	ctx := context.Background()

	seedAuditEvents(t, store, ctx, 20)

	got, err := store.Query(ctx, api.AuditScope{}, time.Time{}, 5)
	if err != nil {
		t.Fatalf("Query(limit=5) returned error: %v", err)
	}
	if len(got) != 5 {
		t.Errorf("limit=5 should return exactly 5 rows, got %d", len(got))
	}
}

// seedAuditEventsBulk inserts n audit rows inside a single transaction.
// Per-row Append commits one transaction per row, which dominates wall
// time at 10k+ rows; bundling into one commit keeps the ceiling-clamp
// integration test sub-second on SQLite. Mirrors the SQLite branch of
// AuditStore.Append; admin tests are SQLite-only via setupAdminTestDB.
func seedAuditEventsBulk(t *testing.T, db storage.DB, ctx context.Context, n int) {
	t.Helper()
	if db.Backend() != storage.BackendSQLite {
		t.Fatalf("seedAuditEventsBulk: requires SQLite backend, got %s", db.Backend())
	}
	const q = `INSERT INTO audit_events
		(id, occurred_at, actor_user_id, actor_role, action, target_type,
		 target_id, target_org_id, source_ip, user_agent, details)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	base := time.Now().UTC()
	for i := range n {
		occurredAt := base.Add(time.Duration(-i) * time.Millisecond).
			Format("2006-01-02T15:04:05.000Z")
		if _, err := tx.ExecContext(ctx, q,
			uuid.New().String(), occurredAt,
			nil, nil, "test.event", nil, nil, nil, nil, nil, "{}",
		); err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				err = errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
			}
			t.Fatalf("seed exec[%d]: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// TestAuditStoreQuery_ClampsAtSQLLimit is the end-to-end assertion the
// helper unit test cannot make: with more rows than the ceiling available
// in the table, Query at limit=MaxInt must return exactly the ceiling.
// A regression that drops clampAuditQueryLimit from Query (or inlines
// only the floor) fails here.
func TestAuditStoreQuery_ClampsAtSQLLimit(t *testing.T) {
	db := setupAdminTestDB(t)
	store := NewAuditStore(db)
	ctx := context.Background()

	seedAuditEventsBulk(t, db, ctx, maxAuditQueryLimit+5)

	got, err := store.Query(ctx, api.AuditScope{}, time.Time{}, math.MaxInt)
	if err != nil {
		t.Fatalf("Query(limit=MaxInt) returned error: %v", err)
	}
	if len(got) != maxAuditQueryLimit {
		t.Errorf("seeded %d rows, Query(limit=MaxInt) returned %d, want %d",
			maxAuditQueryLimit+5, len(got), maxAuditQueryLimit)
	}
}
