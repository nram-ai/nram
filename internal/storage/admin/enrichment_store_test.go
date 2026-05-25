package admin

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage"
)

// seedEnrichmentFixture wires up org → user namespace → project namespace +
// project row + memory + enrichment_queue rows. Returns the user namespace
// ID (passed to SelfQueueStatus), the org ID (passed to OrgQueueStatus),
// the project ID (asserted on items), and the project name (asserted on
// self-tier responses, expected empty on org and admin-tier responses).
func seedEnrichmentFixture(t *testing.T, db storage.DB, ctx context.Context) (
	userNsID uuid.UUID, orgID uuid.UUID, projectID uuid.UUID, projectName string,
) {
	t.Helper()
	var orgNsID uuid.UUID
	orgID, orgNsID = insertOrgWithNamespace(t, db, ctx)

	userNsID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userNsID.String(), "alice", userNsID.String(), "user",
		"test-org/"+userNsID.String(), 1, orgNsID.String())

	projNsID := uuid.New()
	projectName = "alice-proj"
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), projectName, projectName, "project",
		"test-org/"+userNsID.String()+"/"+projectName, 2, userNsID.String())

	projectID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		projectID.String(), projectName, projectName, projNsID.String(), userNsID.String())

	memID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		memID.String(), projNsID.String(), "x")

	for _, status := range []string{"pending", "failed"} {
		execSeed(t, db, ctx,
			"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status) VALUES (?, ?, ?, ?)",
			uuid.New().String(), memID.String(), projNsID.String(), status)
	}
	return
}

// TestEnrichment_SelfQueueStatus_PopulatesProjectFields asserts the
// JOIN-based self path emits both project_id and project_name on each item.
// Only self-tier callers see project names (they own every returned
// project); org and system tiers leave the field empty. The field powers
// the Project column on the user-facing enrichment queue.
func TestEnrichment_SelfQueueStatus_PopulatesProjectFields(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

	userNsID, _, projectID, projectName := seedEnrichmentFixture(t, db, ctx)

	resp, err := store.SelfQueueStatus(ctx, userNsID)
	if err != nil {
		t.Fatalf("SelfQueueStatus: %v", err)
	}
	if resp == nil || len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %+v", resp)
	}
	for i, item := range resp.Items {
		if item.ProjectID == nil {
			t.Errorf("item[%d].ProjectID is nil; want %s", i, projectID)
			continue
		}
		if *item.ProjectID != projectID {
			t.Errorf("item[%d].ProjectID: got %s want %s", i, *item.ProjectID, projectID)
		}
		if item.ProjectName != projectName {
			t.Errorf("item[%d].ProjectName: got %q want %q", i, item.ProjectName, projectName)
		}
	}
}

// TestEnrichment_QueueStatus_AdminEmitsProjectIDOnly asserts the admin
// (system-tier) path populates project_id but leaves project_name empty —
// matching the privacy posture: cross-tenant admin views show UUIDs only.
func TestEnrichment_QueueStatus_AdminEmitsProjectIDOnly(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

	_, _, projectID, _ := seedEnrichmentFixture(t, db, ctx)

	resp, err := store.QueueStatus(ctx)
	if err != nil {
		t.Fatalf("QueueStatus: %v", err)
	}
	if resp == nil || len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %+v", resp)
	}
	for i, item := range resp.Items {
		if item.ProjectID == nil {
			t.Errorf("item[%d].ProjectID is nil; admin tier should still surface project_id", i)
			continue
		}
		if *item.ProjectID != projectID {
			t.Errorf("item[%d].ProjectID: got %s want %s", i, *item.ProjectID, projectID)
		}
		if item.ProjectName != "" {
			t.Errorf("item[%d].ProjectName: got %q, expected empty (admin tier emits UUID only)", i, item.ProjectName)
		}
	}
}

// seedSecondOrgEnrichmentFixture mirrors seedEnrichmentFixture but takes
// caller-supplied slugs/names so two orgs can coexist in the same test DB
// without colliding on UNIQUE(parent_id, slug) on namespaces or UNIQUE on
// organizations.slug. statuses controls how many queue rows are seeded
// (one per status string). Returns the org ID and project ID so the test
// can scope assertions to each tenant.
func seedSecondOrgEnrichmentFixture(
	t *testing.T, db storage.DB, ctx context.Context,
	orgSlug, userSlug, projectSlug string, statuses []string,
) (orgID uuid.UUID, projectID uuid.UUID) {
	t.Helper()
	orgNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth) VALUES (?, ?, ?, ?, ?, ?)",
		orgNsID.String(), orgSlug, orgSlug, "org", orgSlug, 0)
	orgID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO organizations (id, name, slug, namespace_id) VALUES (?, ?, ?, ?)",
		orgID.String(), orgSlug, orgSlug, orgNsID.String())

	userNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userNsID.String(), userSlug, userNsID.String(), "user",
		orgSlug+"/"+userNsID.String(), 1, orgNsID.String())

	projNsID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), projectSlug, projectSlug, "project",
		orgSlug+"/"+userNsID.String()+"/"+projectSlug, 2, userNsID.String())

	projectID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		projectID.String(), projectSlug, projectSlug, projNsID.String(), userNsID.String())

	memID := uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO memories (id, namespace_id, content) VALUES (?, ?, ?)",
		memID.String(), projNsID.String(), "x")

	for _, status := range statuses {
		execSeed(t, db, ctx,
			"INSERT INTO enrichment_queue (id, memory_id, namespace_id, status) VALUES (?, ?, ?, ?)",
			uuid.New().String(), memID.String(), projNsID.String(), status)
	}
	return
}

// TestEnrichment_OrgQueueStatus_EmitsProjectIDOnly asserts the org-tier path
// populates project_id but leaves project_name empty. An org_owner browsing
// the org enrichment queue must not learn the names of projects owned by
// other users in the org; only the project UUID surfaces. Mirrors the
// system-tier privacy posture covered by
// TestEnrichment_QueueStatus_AdminEmitsProjectIDOnly above.
//
// Cross-tenant control: a second org is seeded with three additional queue
// rows under a distinct project. The test asserts OrgQueueStatus on the
// first org returns exactly the first org's rows (no second-org leakage)
// and that resp.Counts is scoped to the first org's subtree (not the full
// DB). The counts loop runs separate queries from the items SELECT, so
// asserting both guards both code paths against a scoping regression.
//
// Runs against both sqlite and Postgres via adminTestBackends so the
// $1/$2-placeholder branch of OrgQueueStatus also gets coverage.
func TestEnrichment_OrgQueueStatus_EmitsProjectIDOnly(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()

			queueRepo := storage.NewEnrichmentQueueRepo(db)
			settingsRepo := storage.NewSettingsRepo(db)
			store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

			_, orgAID, orgAProjectID, orgAProjectName := seedEnrichmentFixture(t, db, ctx)

			// Cross-tenant control data: a separate org with its own
			// project and queue rows. None of these must surface in
			// the orgA query.
			orgBID, orgBProjectID := seedSecondOrgEnrichmentFixture(
				t, db, ctx,
				"other-org", "bob", "bob-proj",
				[]string{"pending", "processing", "completed"},
			)

			resp, err := store.OrgQueueStatus(ctx, orgAID)
			if err != nil {
				t.Fatalf("OrgQueueStatus(orgA): %v", err)
			}
			if resp == nil || len(resp.Items) != 2 {
				t.Fatalf("orgA: expected 2 items, got %+v", resp)
			}
			for i, item := range resp.Items {
				if item.ProjectID == nil {
					t.Errorf("orgA item[%d].ProjectID is nil; org tier should still surface project_id", i)
					continue
				}
				if *item.ProjectID == orgBProjectID {
					t.Errorf("orgA item[%d].ProjectID leaked orgB's project %s", i, orgBProjectID)
					continue
				}
				if *item.ProjectID != orgAProjectID {
					t.Errorf("orgA item[%d].ProjectID: got %s want %s", i, *item.ProjectID, orgAProjectID)
				}
				if item.ProjectName != "" {
					t.Errorf("orgA item[%d].ProjectName: got %q, expected empty (org tier emits UUID only — fixture name was %q)", i, item.ProjectName, orgAProjectName)
				}
			}

			// Counts must be scoped to orgA's subtree. seedEnrichmentFixture
			// inserts one pending and one failed row; orgB's three rows
			// (pending+processing+completed) must not bleed in.
			if resp.Counts.Pending != 1 {
				t.Errorf("orgA Counts.Pending: got %d want 1 (leak from orgB's pending row would push this to 2)", resp.Counts.Pending)
			}
			if resp.Counts.Failed != 1 {
				t.Errorf("orgA Counts.Failed: got %d want 1", resp.Counts.Failed)
			}
			if resp.Counts.Processing != 0 {
				t.Errorf("orgA Counts.Processing: got %d want 0 (orgB has 1 processing row that must not leak)", resp.Counts.Processing)
			}
			if resp.Counts.Completed != 0 {
				t.Errorf("orgA Counts.Completed: got %d want 0 (orgB has 1 completed row that must not leak)", resp.Counts.Completed)
			}

			// Sanity check the other direction: querying orgB returns
			// only its 3 rows, none of orgA's.
			respB, err := store.OrgQueueStatus(ctx, orgBID)
			if err != nil {
				t.Fatalf("OrgQueueStatus(orgB): %v", err)
			}
			if respB == nil || len(respB.Items) != 3 {
				t.Fatalf("orgB: expected 3 items, got %+v", respB)
			}
			for i, item := range respB.Items {
				if item.ProjectID == nil {
					continue
				}
				if *item.ProjectID == orgAProjectID {
					t.Errorf("orgB item[%d].ProjectID leaked orgA's project %s", i, orgAProjectID)
				}
				if item.ProjectName != "" {
					t.Errorf("orgB item[%d].ProjectName: got %q, expected empty", i, item.ProjectName)
				}
			}
		})
	}
}
