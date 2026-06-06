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

// seedOrgWithTwoUsers wires up an org with two users, each with their own
// user namespace beneath the org. Returns orgID and the two user IDs/ns IDs
// in declaration order. Mirrors how production users are created
// (org/{user_id}) so per-user namespace-prefix queries find the right rows.
func seedOrgWithTwoUsers(t *testing.T, db storage.DB, ctx context.Context) (
	orgID, userAID, userANsID, userBID, userBNsID uuid.UUID,
) {
	t.Helper()
	orgID, orgNsID := insertOrgWithNamespace(t, db, ctx)

	makeUser := func(email string) (uuid.UUID, uuid.UUID) {
		userID := uuid.New()
		nsID := uuid.New()
		execSeed(t, db, ctx,
			"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
			nsID.String(), email, nsID.String(), "user", "test-org/"+nsID.String(), 1, orgNsID.String())
		execSeed(t, db, ctx,
			"INSERT INTO users (id, email, org_id, namespace_id, role) VALUES (?, ?, ?, ?, ?)",
			userID.String(), email, orgID.String(), nsID.String(), "member")
		return userID, nsID
	}

	userAID, userANsID = makeUser("a@example.com")
	userBID, userBNsID = makeUser("b@example.com")
	return
}

func TestAggregates_UserBreakdown_OrgScoped(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	store := NewAggregatesStore(db)

	orgID, _, userANsID, _, userBNsID := seedOrgWithTwoUsers(t, db, ctx)

	// Both UserBreakdown and the production OrgBreakdown use the same
	// "<path>/%" prefix subquery, which counts strict descendants only;
	// rows at the user's own namespace_id do NOT match. Production user
	// data lives in project sub-namespaces, so we seed the same shape.

	// User A: 1 project under their namespace, with 2 memories and 1 entity
	// at the project namespace.
	projANsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projANsID.String(), "alice-proj", "alice-proj", "project",
		"test-org/"+userANsID.String()+"/alice-proj", 2, userANsID.String())
	execSeed(t, db, ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		uuid.New().String(), "alice-proj", "alice-proj", projANsID.String(), userANsID.String())
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		uuid.New().String(), projANsID.String(), "alice proj memory 1")
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		uuid.New().String(), projANsID.String(), "alice proj memory 2")
	execSeed(t, db, ctx,
		"INSERT INTO entities (id, namespace_id, name, canonical, entity_type) VALUES (?, ?, ?, ?, ?)",
		uuid.New().String(), projANsID.String(), "Carol", "carol", "person")

	// User B: 1 project, 1 memory, no entities. Confirms per-user scoping;
	// user B's counts must not bleed into user A's row.
	projBNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projBNsID.String(), "bob-proj", "bob-proj", "project",
		"test-org/"+userBNsID.String()+"/bob-proj", 2, userBNsID.String())
	execSeed(t, db, ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		uuid.New().String(), "bob-proj", "bob-proj", projBNsID.String(), userBNsID.String())
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		uuid.New().String(), projBNsID.String(), "bob proj memory")

	rows, err := store.UserBreakdown(ctx, orgID)
	if err != nil {
		t.Fatalf("UserBreakdown: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 user rows, got %d (%+v)", len(rows), rows)
	}

	// Rows are ordered by email: a@, b@.
	if rows[0].Email != "a@example.com" {
		t.Errorf("row 0 email: got %q want a@example.com", rows[0].Email)
	}
	if rows[0].TotalMemories != 2 {
		t.Errorf("user A memories: got %d want 2", rows[0].TotalMemories)
	}
	if rows[0].TotalProjects != 1 {
		t.Errorf("user A projects: got %d want 1", rows[0].TotalProjects)
	}
	if rows[0].TotalEntities != 1 {
		t.Errorf("user A entities: got %d want 1", rows[0].TotalEntities)
	}

	if rows[1].Email != "b@example.com" {
		t.Errorf("row 1 email: got %q want b@example.com", rows[1].Email)
	}
	if rows[1].TotalMemories != 1 {
		t.Errorf("user B memories: got %d want 1", rows[1].TotalMemories)
	}
	if rows[1].TotalProjects != 1 {
		t.Errorf("user B projects: got %d want 1", rows[1].TotalProjects)
	}
	if rows[1].TotalEntities != 0 {
		t.Errorf("user B entities: got %d want 0", rows[1].TotalEntities)
	}
}

func TestAggregates_OrgEnrichmentQueueStats(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()
	store := NewAggregatesStore(db)

	orgID, orgNsID := insertOrgWithNamespace(t, db, ctx)

	// Project namespace under the org so memory + queue rows fall under the
	// org's path prefix.
	projNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), "p", "p", "project", "test-org/p", 1, orgNsID.String())

	// One memory per queue row, all under the project ns. A distinct memory per
	// row is required because the partial unique index forbids two pending jobs
	// for the same memory; the two pending rows must therefore span two memories.
	for _, status := range []string{"pending", "pending", "processing", "failed"} {
		memID := uuid.New()
		execSeed(t, db, ctx,
			"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
			memID.String(), projNsID.String(), "x")
		execSeed(t, db, ctx,
			"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status) VALUES (?, ?, ?, ?)",
			uuid.New().String(), memID.String(), projNsID.String(), status)
	}

	stats, err := store.OrgEnrichmentQueueStats(ctx, orgID)
	if err != nil {
		t.Fatalf("OrgEnrichmentQueueStats: %v", err)
	}
	if stats == nil {
		t.Fatal("stats nil")
	}
	if stats.Pending != 2 {
		t.Errorf("Pending: got %d want 2", stats.Pending)
	}
	if stats.Processing != 1 {
		t.Errorf("Processing: got %d want 1", stats.Processing)
	}
	if stats.Failed != 1 {
		t.Errorf("Failed: got %d want 1", stats.Failed)
	}
}
