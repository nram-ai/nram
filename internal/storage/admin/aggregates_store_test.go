package admin

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage"
)

// seedOrgWithMemoryAndGraph wires up a complete org → project namespace
// hierarchy with a memory, an entity, and a relationship so the aggregate
// queries that flow through namespacePrefixSubquery have rows to return.
func seedOrgWithMemoryAndGraph(t *testing.T, db storage.DB, ctx context.Context) (orgID uuid.UUID) {
	t.Helper()
	orgID, orgNsID := insertOrgWithNamespace(t, db, ctx)

	projNsID := uuid.New()
	if _, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), "proj", "proj", "project", "test-org/proj", 1, orgNsID.String()); err != nil {
		t.Fatalf("insert project namespace: %v", err)
	}

	memID := uuid.New()
	if _, err := db.Exec(ctx,
		`INSERT INTO memories (id, namespace_id, content, access_count) VALUES (?, ?, ?, ?)`,
		memID.String(), projNsID.String(), "hello world", 7); err != nil {
		t.Fatalf("insert memory: %v", err)
	}

	srcID := uuid.New()
	tgtID := uuid.New()
	for _, e := range []struct {
		id   uuid.UUID
		name string
	}{{srcID, "alice"}, {tgtID, "bob"}} {
		if _, err := db.Exec(ctx,
			`INSERT INTO entities (id, namespace_id, name, canonical, entity_type) VALUES (?, ?, ?, ?, ?)`,
			e.id.String(), projNsID.String(), e.name, e.name, "person"); err != nil {
			t.Fatalf("insert entity %s: %v", e.name, err)
		}
	}

	if _, err := db.Exec(ctx,
		`INSERT INTO relationships (id, namespace_id, source_id, target_id, relation, weight) VALUES (?, ?, ?, ?, ?, ?)`,
		uuid.New().String(), projNsID.String(), srcID.String(), tgtID.String(), "knows", 1.0); err != nil {
		t.Fatalf("insert relationship: %v", err)
	}

	return orgID
}

// Each of these methods exercises namespacePrefixSubquery with an org-scoped
// orgID. Before the alias fix the produced SQL referenced an undefined alias
// `o` (the JOIN aliased the parent table as `p`), making every org-scoped
// admin tier-B query 500. The tests assert the queries succeed AND return the
// seeded row, since a silently-empty result would also count as "no error."

func TestAggregates_OrgMemoryCounts_OrgScoped(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	store := NewAggregatesStore(db)

	orgID := seedOrgWithMemoryAndGraph(t, db, ctx)
	counts, err := store.OrgMemoryCounts(ctx, orgID)
	if err != nil {
		t.Fatalf("OrgMemoryCounts: %v", err)
	}
	if counts.Total != 1 {
		t.Errorf("Total: got %d, want 1", counts.Total)
	}
	if counts.Active != 1 {
		t.Errorf("Active: got %d, want 1", counts.Active)
	}
}

func TestAggregates_RecallDistribution_OrgScoped(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	store := NewAggregatesStore(db)

	orgID := seedOrgWithMemoryAndGraph(t, db, ctx)
	dist, err := store.RecallDistribution(ctx, &orgID)
	if err != nil {
		t.Fatalf("RecallDistribution: %v", err)
	}
	// One memory with access_count=7 lands in the "3-10" bucket; all five
	// declared buckets must be present (zero-filled).
	if got := len(dist); got != 5 {
		t.Errorf("len(dist): got %d, want 5", got)
	}
	var sum int
	for _, b := range dist {
		sum += b.Count
	}
	if sum != 1 {
		t.Errorf("total bucket count: got %d, want 1", sum)
	}
}

func TestAggregates_ProjectBreakdown_OrgScoped(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	store := NewAggregatesStore(db)

	orgID := seedOrgWithMemoryAndGraph(t, db, ctx)
	// ProjectBreakdown joins projects → namespaces; insert a project row keyed
	// on the project namespace so the breakdown has something to return.
	projNsID := uuid.New()
	if _, err := db.Exec(ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, (SELECT id FROM namespaces WHERE slug='test-org'))",
		projNsID.String(), "p2", "p2", "project", "test-org/p2", 1); err != nil {
		t.Fatalf("insert project ns: %v", err)
	}
	if _, err := db.Exec(ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		uuid.New().String(), "p2", "p2", projNsID.String(), projNsID.String()); err != nil {
		t.Fatalf("insert project: %v", err)
	}

	rows, err := store.ProjectBreakdown(ctx, orgID)
	if err != nil {
		t.Fatalf("ProjectBreakdown: %v", err)
	}
	if len(rows) == 0 {
		t.Errorf("expected at least one project row, got 0")
	}
}

func TestAggregates_EntityTypeHistogram_OrgScoped(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	store := NewAggregatesStore(db)

	orgID := seedOrgWithMemoryAndGraph(t, db, ctx)
	hist, err := store.EntityTypeHistogram(ctx, &orgID)
	if err != nil {
		t.Fatalf("EntityTypeHistogram: %v", err)
	}
	if len(hist) != 1 || hist[0].Type != "person" || hist[0].Count != 2 {
		t.Errorf("hist: got %+v, want [{person 2}]", hist)
	}
}

func TestAggregates_RelationshipTypeHistogram_OrgScoped(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	store := NewAggregatesStore(db)

	orgID := seedOrgWithMemoryAndGraph(t, db, ctx)
	hist, err := store.RelationshipTypeHistogram(ctx, &orgID)
	if err != nil {
		t.Fatalf("RelationshipTypeHistogram: %v", err)
	}
	if len(hist) != 1 || hist[0].Type != "knows" || hist[0].Count != 1 {
		t.Errorf("hist: got %+v, want [{knows 1}]", hist)
	}
}

func TestAggregates_ActivityHistogram_OrgScoped(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	store := NewAggregatesStore(db)

	orgID := seedOrgWithMemoryAndGraph(t, db, ctx)
	rows, err := store.ActivityHistogram(ctx, &orgID, 30)
	if err != nil {
		t.Fatalf("ActivityHistogram: %v", err)
	}
	var sum int
	for _, b := range rows {
		sum += b.Count
	}
	if sum != 1 {
		t.Errorf("total daily count: got %d, want 1", sum)
	}
}
