package admin

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// fixedSettings is a DreamSettingsResolver stub returning the production
// defaults so decorateAll doesn't need a real settings repo. dream-cycle
// staleness / abandonable flags are not what these tests assert on, but
// decorateAll runs unconditionally inside OrgListCycles so the stub keeps
// the call graph intact.
type fixedSettings struct{}

func (fixedSettings) ResolveIntWithDefault(_ context.Context, _, _ string) int {
	return 1800
}

// seedDreamOrgFixture wires up org → user namespace → project namespace +
// project row + dream_cycle, all under the given org slug. Returns the org
// ID, project ID, and project name so tests can assert against them.
func seedDreamOrgFixture(
	t *testing.T, db storage.DB, ctx context.Context,
	orgSlug, userSlug, projectSlug string,
) (orgID, projectID uuid.UUID, projectName string) {
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
	projectName = projectSlug
	execSeed(t, db, ctx,
		"INSERT INTO namespaces (id, name, slug, kind, path, depth, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
		projNsID.String(), projectSlug, projectSlug, "project",
		orgSlug+"/"+userNsID.String()+"/"+projectSlug, 2, userNsID.String())

	projectID = uuid.New()
	execSeed(t, db, ctx,
		"INSERT INTO projects (id, name, slug, namespace_id, owner_namespace_id) VALUES (?, ?, ?, ?, ?)",
		projectID.String(), projectName, projectSlug, projNsID.String(), userNsID.String())

	cycleRepo := storage.NewDreamCycleRepo(db)
	cycle := &model.DreamCycle{
		ID:          uuid.New(),
		ProjectID:   projectID,
		NamespaceID: projNsID,
		TokenBudget: 10_000,
	}
	if err := cycleRepo.Create(ctx, cycle); err != nil {
		t.Fatalf("create dream cycle: %v", err)
	}
	return
}

// TestDream_OrgListCycles_EmitsProjectIDOnly asserts the store-level
// contract: OrgListCycles returns cycles whose ProjectName is empty
// regardless of the JOIN data underneath. This guards against a future
// regression where a post-scan ProjectName re-lookup gets inserted in
// OrgListCycles (e.g. analogous to the single-project branch of
// handleMeDreamCyclesList), which would bypass the repo-level
// withProjectName=false guard and re-leak names to org owners. Also
// confirms cross-tenant isolation: a second org's cycles must not appear
// in the first org's response.
//
// Runs against both sqlite and Postgres via adminTestBackends so both
// placeholder forms of ListByNamespacePathPrefix get coverage at the
// store boundary.
func TestDream_OrgListCycles_EmitsProjectIDOnly(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()

			cycleRepo := storage.NewDreamCycleRepo(db)
			store := &DreamAdminStore{
				cycleRepo: cycleRepo,
				settings:  fixedSettings{},
				db:        db,
			}

			orgAID, orgAProjectID, orgAProjectName := seedDreamOrgFixture(t, db, ctx, "test-org", "alice", "alice-proj")
			_, orgBProjectID, _ := seedDreamOrgFixture(t, db, ctx, "other-org", "bob", "bob-proj")

			cycles, err := store.OrgListCycles(ctx, orgAID, 50)
			if err != nil {
				t.Fatalf("OrgListCycles(orgA): %v", err)
			}
			if len(cycles) != 1 {
				t.Fatalf("orgA: expected 1 cycle, got %d (%+v)", len(cycles), cycles)
			}
			c := cycles[0]
			if c.ProjectID != orgAProjectID {
				if c.ProjectID == orgBProjectID {
					t.Errorf("orgA leaked orgB's cycle (ProjectID=%s)", c.ProjectID)
				} else {
					t.Errorf("orgA ProjectID: got %s want %s", c.ProjectID, orgAProjectID)
				}
			}
			if c.ProjectName != "" {
				t.Errorf("orgA ProjectName: got %q, expected empty (org tier emits UUID only — fixture name was %q)", c.ProjectName, orgAProjectName)
			}
		})
	}
}
