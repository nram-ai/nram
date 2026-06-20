package storage

import (
	"context"
	"encoding/json"
	"io/fs"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/migrations"
)

// TestMigration000056_PrunesInvalidTransitiveEdges exercises the actual
// 000056 cleanup migration (read from the embedded migration FS, not a copy)
// against both dialects. It seeds direct edges plus a mix of valid and invalid
// inferred (source=transitive) edges and asserts that only the invalid ones
// are removed while direct edges and the valid inferred edge survive.
func TestMigration000056_PrunesInvalidTransitiveEdges(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		truncateAllForTest(t, db)

		nsID := createTestNamespace(t, ctx, db)
		a := createTestEntity(t, ctx, db, nsID, "A")
		b := createTestEntity(t, ctx, db, nsID, "B")
		c := createTestEntity(t, ctx, db, nsID, "C")
		x := createTestEntity(t, ctx, db, nsID, "X")
		y := createTestEntity(t, ctx, db, nsID, "Y")
		z := createTestEntity(t, ctx, db, nsID, "Z")

		repo := NewRelationshipRepo(db)
		direct := json.RawMessage(`{}`)
		inferred := json.RawMessage(`{"source":"transitive"}`)
		mk := func(src, tgt uuid.UUID, rel string, props json.RawMessage) {
			t.Helper()
			if err := repo.Create(ctx, &model.Relationship{
				NamespaceID: nsID, SourceID: src, TargetID: tgt,
				Relation: rel, Weight: 0.9, Properties: props,
			}); err != nil {
				t.Fatalf("seed %s->%s %q: %v", src, tgt, rel, err)
			}
		}

		// Direct edges: a same-relation "part of" path A->B->C (backs a valid
		// inference) plus an unrelated direct "wife of" edge that must never be
		// touched by the migration (it is not source=transitive).
		mk(a, b, "part of", direct)
		mk(b, c, "part of", direct)
		mk(x, y, "wife of", direct)

		// Inferred edges:
		//  - A->C "part of": VALID (in-set relation + backing same-relation path) → keep
		//  - X->Z "wife of": INVALID (relation not in transitive set) → delete
		//  - A->Z "part of": INVALID (in-set relation but no backing path) → delete
		mk(a, c, "part of", inferred)
		mk(x, z, "wife of", inferred)
		mk(a, z, "part of", inferred)

		// Run the real migration SQL for this backend.
		var fsys fs.FS = migrations.SQLiteFS
		path := "sqlite/000056_prune_invalid_transitive_edges.up.sql"
		if db.Backend() == BackendPostgres {
			fsys = migrations.PostgresFS
			path = "postgres/000056_prune_invalid_transitive_edges.up.sql"
		}
		sqlBytes, err := fs.ReadFile(fsys, path)
		if err != nil {
			t.Fatalf("read migration: %v", err)
		}
		if _, err := db.Exec(ctx, string(sqlBytes)); err != nil {
			t.Fatalf("apply migration: %v", err)
		}

		rels, err := repo.ListByNamespace(ctx, nsID)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		type tri struct {
			s, t uuid.UUID
			rel  string
		}
		transitive := map[tri]bool{}
		directCount := 0
		for _, r := range rels {
			var p struct {
				Source string `json:"source"`
			}
			_ = json.Unmarshal(r.Properties, &p)
			if p.Source == "transitive" {
				transitive[tri{r.SourceID, r.TargetID, r.Relation}] = true
			} else {
				directCount++
			}
		}

		if !transitive[tri{a, c, "part of"}] {
			t.Errorf("valid inferred edge A--part of-->C was wrongly deleted")
		}
		if transitive[tri{x, z, "wife of"}] {
			t.Errorf("invalid inferred edge X--wife of-->Z (non-transitive relation) was not deleted")
		}
		if transitive[tri{a, z, "part of"}] {
			t.Errorf("invalid inferred edge A--part of-->Z (no backing path) was not deleted")
		}
		if len(transitive) != 1 {
			t.Errorf("remaining inferred edges = %d, want exactly 1 (A->C): %v", len(transitive), transitive)
		}
		if directCount != 3 {
			t.Errorf("direct edges = %d, want 3 (migration must not touch non-transitive edges)", directCount)
		}
	})
}
