package migration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
)

// insertRel writes a relationship row directly (FK is OFF in the embedded test
// DB, so namespace/entity rows are unnecessary). valid_from is explicit so
// callers can force or avoid unique-key collisions.
func insertRel(t *testing.T, db *sql.DB, ns, src, tgt, relation, validFrom string, weight float64) string {
	t.Helper()
	id := uuid.New().String()
	if _, err := db.Exec(
		`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight, valid_from, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, ns, src, tgt, relation, weight, validFrom, validFrom); err != nil {
		t.Fatalf("insert relationship: %v", err)
	}
	return id
}

func TestCanonicalizeRelations_MergesVariants(t *testing.T) {
	db := runEmbeddedSQLiteMigrations(t)
	ctx := context.Background()

	ns := uuid.New().String()
	src := uuid.New().String()
	tgt := uuid.New().String()
	vf := "2026-01-02T03:04:05Z"

	// Two formatting variants of the same edge that collide once canonicalized,
	// plus an unrelated edge that should be left alone.
	insertRel(t, db, ns, src, tgt, "related_to", vf, 0.4)
	insertRel(t, db, ns, src, tgt, "related to", vf, 0.7)
	other := insertRel(t, db, ns, src, tgt, "knows", vf, 0.5)

	changed, err := CanonicalizeRelations(ctx, db, "sqlite")
	if err != nil {
		t.Fatalf("CanonicalizeRelations: %v", err)
	}
	if changed == 0 {
		t.Fatal("expected changes, got 0")
	}

	// The two variants collapse to one canonical "related to" row at max weight.
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM relationships WHERE source_id = ? AND target_id = ? AND relation = 'related to'`,
		src, tgt).Scan(&count); err != nil {
		t.Fatalf("count related: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 merged 'related to' row, got %d", count)
	}
	var weight float64
	if err := db.QueryRow(
		`SELECT weight FROM relationships WHERE source_id = ? AND target_id = ? AND relation = 'related to'`,
		src, tgt).Scan(&weight); err != nil {
		t.Fatalf("read weight: %v", err)
	}
	if weight != 0.7 {
		t.Errorf("merged weight = %v, want 0.7", weight)
	}

	// No surviving variant labels.
	for _, variant := range []string{"related_to"} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM relationships WHERE relation = ?`, variant).Scan(&n); err != nil {
			t.Fatalf("count variant %q: %v", variant, err)
		}
		if n != 0 {
			t.Errorf("variant %q should be gone, found %d", variant, n)
		}
	}

	// The unrelated edge is untouched.
	var otherRel string
	if err := db.QueryRow(`SELECT relation FROM relationships WHERE id = ?`, other).Scan(&otherRel); err != nil {
		t.Fatalf("read other: %v", err)
	}
	if otherRel != "knows" {
		t.Errorf("unrelated edge changed: relation = %q, want 'knows'", otherRel)
	}
}

func TestCanonicalizeRelations_Idempotent(t *testing.T) {
	db := runEmbeddedSQLiteMigrations(t)
	ctx := context.Background()

	ns := uuid.New().String()
	src := uuid.New().String()
	tgt := uuid.New().String()
	vf := "2026-01-02T03:04:05Z"
	insertRel(t, db, ns, src, tgt, "could_expose", vf, 0.3)
	insertRel(t, db, ns, src, tgt, "could expose", vf, 0.9)

	if _, err := CanonicalizeRelations(ctx, db, "sqlite"); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	changed, err := CanonicalizeRelations(ctx, db, "sqlite")
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if changed != 0 {
		t.Errorf("expected 0 changes on idempotent re-run, got %d", changed)
	}
}

func TestCanonicalizeRelations_EmptyTable(t *testing.T) {
	db := runEmbeddedSQLiteMigrations(t)
	changed, err := CanonicalizeRelations(context.Background(), db, "sqlite")
	if err != nil {
		t.Fatalf("empty table: %v", err)
	}
	if changed != 0 {
		t.Errorf("expected 0 changes on empty table (fresh install), got %d", changed)
	}
}
