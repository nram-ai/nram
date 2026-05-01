package admin

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage"
)

// seedEnrichmentFixture wires up org → user namespace → project namespace +
// project row + memory + enrichment_queue rows. Returns the user namespace
// ID (passed to SelfQueueStatus), the project ID (asserted on items), and
// the project name (asserted on self-tier responses, expected empty on
// admin-tier responses).
func seedEnrichmentFixture(t *testing.T, db storage.DB, ctx context.Context) (
	userNsID uuid.UUID, projectID uuid.UUID, projectName string,
) {
	t.Helper()
	_, orgNsID := insertOrgWithNamespace(t, db, ctx)

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
// Self/org-tier callers see their own project names; the field powers the
// new Project column on the user-facing enrichment queue.
func TestEnrichment_SelfQueueStatus_PopulatesProjectFields(t *testing.T) {
	db := setupAdminTestDB(t)
	ctx := context.Background()

	queueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	store := NewEnrichmentAdminStore(queueRepo, settingsRepo, nil, db)

	userNsID, projectID, projectName := seedEnrichmentFixture(t, db, ctx)

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

	_, projectID, _ := seedEnrichmentFixture(t, db, ctx)

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
