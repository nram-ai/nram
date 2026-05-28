package mcp

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// These tests pin down the per-tier scoping logic that gates every share-
// bearer MCP tool call. A regression here either silently widens access
// (granting tools the share's tier shouldn't reach) or narrows access
// (locking out callers the share legitimately covers).

func makeShareAC(projectID uuid.UUID, perm model.SharePermission) *auth.AuthContext {
	shareID := uuid.New()
	return &auth.AuthContext{
		UserID:       uuid.New(),
		ShareTokenID: &shareID,
		ShareGrants: []model.ProjectGrant{{
			ProjectID:  projectID,
			Permission: perm,
		}},
	}
}

func TestShareToolAllowed_TierMatrix(t *testing.T) {
	projectA := uuid.New()

	cases := []struct {
		name    string
		tier    model.SharePermission
		tool    string
		allowed bool
	}{
		{"read tier: recall", model.SharePermissionRead, "recall", true},
		{"read tier: list", model.SharePermissionRead, "list", true},
		{"read tier: get", model.SharePermissionRead, "get", true},
		{"read tier: graph", model.SharePermissionRead, "graph", true},
		{"read tier: list_projects", model.SharePermissionRead, "list_projects", true},
		{"read tier: store rejected", model.SharePermissionRead, "store", false},
		{"read tier: store_batch rejected", model.SharePermissionRead, "store_batch", false},
		{"read tier: update rejected", model.SharePermissionRead, "update", false},
		{"read tier: forget rejected", model.SharePermissionRead, "forget", false},
		{"read tier: delete_project rejected", model.SharePermissionRead, "delete_project", false},
		{"read tier: update_project rejected", model.SharePermissionRead, "update_project", false},

		{"read_store tier: store", model.SharePermissionReadStore, "store", true},
		{"read_store tier: store_batch", model.SharePermissionReadStore, "store_batch", true},
		{"read_store tier: recall still allowed", model.SharePermissionReadStore, "recall", true},
		{"read_store tier: update rejected", model.SharePermissionReadStore, "update", false},
		{"read_store tier: forget rejected", model.SharePermissionReadStore, "forget", false},
		{"read_store tier: delete_project rejected", model.SharePermissionReadStore, "delete_project", false},

		{"read_store_modify tier: update", model.SharePermissionReadStoreModify, "update", true},
		{"read_store_modify tier: forget", model.SharePermissionReadStoreModify, "forget", true},
		{"read_store_modify tier: store still allowed", model.SharePermissionReadStoreModify, "store", true},
		{"read_store_modify tier: recall still allowed", model.SharePermissionReadStoreModify, "recall", true},
		{"read_store_modify tier: delete_project rejected", model.SharePermissionReadStoreModify, "delete_project", false},
		{"read_store_modify tier: update_project rejected", model.SharePermissionReadStoreModify, "update_project", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ac := makeShareAC(projectA, tc.tier)
			ok, _ := shareToolAllowed(ac, tc.tool, projectA)
			if ok != tc.allowed {
				t.Fatalf("shareToolAllowed(tier=%s, tool=%s) = %v, want %v", tc.tier, tc.tool, ok, tc.allowed)
			}
		})
	}
}

func TestShareToolAllowed_OffAllowlistProjectRejected(t *testing.T) {
	projectA := uuid.New()
	projectB := uuid.New()
	ac := makeShareAC(projectA, model.SharePermissionReadStoreModify)

	if ok, _ := shareToolAllowed(ac, "recall", projectB); ok {
		t.Fatal("recall on off-allowlist project should be rejected even at the highest tier")
	}
	if ok, _ := shareToolAllowed(ac, "store", projectB); ok {
		t.Fatal("store on off-allowlist project should be rejected")
	}
	if ok, _ := shareToolAllowed(ac, "forget", projectB); ok {
		t.Fatal("forget on off-allowlist project should be rejected")
	}
}

func TestShareToolAllowed_NonShareCallerPassesThrough(t *testing.T) {
	// Non-share callers (no ShareTokenID) are gated by other middleware
	// (role checks, project access). shareToolAllowed must not falsely
	// reject them.
	ac := &auth.AuthContext{UserID: uuid.New()}
	if ok, _ := shareToolAllowed(ac, "store", uuid.New()); !ok {
		t.Fatal("non-share caller should pass shareToolAllowed unconditionally")
	}
}

func TestShareToolAllowed_NilAuthContext(t *testing.T) {
	// nil ac shouldn't panic and shouldn't claim to allow anything (the
	// caller's own auth.FromContext nil-check is what actually rejects).
	if ok, _ := shareToolAllowed(nil, "recall", uuid.New()); !ok {
		t.Fatal("nil AC should pass shareToolAllowed (rejection happens upstream)")
	}
}

func TestRequireShareProject_RejectsEmptyProjectForShareBearer(t *testing.T) {
	projectA := uuid.New()
	ac := makeShareAC(projectA, model.SharePermissionRead)

	if res := requireShareProject(context.Background(), ac, "recall", "", uuid.Nil); res == nil {
		t.Fatal("requireShareProject must reject empty project for share-bearer (no global fan-out)")
	}
}

func TestRequireShareProject_RejectsOffAllowlist(t *testing.T) {
	projectA := uuid.New()
	projectB := uuid.New()
	ac := makeShareAC(projectA, model.SharePermissionReadStoreModify)

	if res := requireShareProject(context.Background(), ac, "recall", "other", projectB); res == nil {
		t.Fatal("requireShareProject must reject off-allowlist project")
	}
}

func TestRequireShareProject_AllowsInAllowlist(t *testing.T) {
	projectA := uuid.New()
	ac := makeShareAC(projectA, model.SharePermissionRead)

	if res := requireShareProject(context.Background(), ac, "recall", "alpha", projectA); res != nil {
		t.Fatalf("requireShareProject rejected an in-allowlist call: %v", res)
	}
}

func TestRequireShareProject_NonShareCallerPasses(t *testing.T) {
	// Non-share callers should not be affected by share-bearer gates.
	ac := &auth.AuthContext{UserID: uuid.New()}
	if res := requireShareProject(context.Background(), ac, "recall", "", uuid.Nil); res != nil {
		t.Fatalf("requireShareProject must be a no-op for non-share callers: %v", res)
	}
}

func TestShareToolAllowed_AnyProjectMode(t *testing.T) {
	// list_projects passes projectID=uuid.Nil meaning "any project". A share
	// with at least one grant at the required tier is allowed; an empty
	// grant set is not.
	projectA := uuid.New()

	withRead := makeShareAC(projectA, model.SharePermissionRead)
	if ok, _ := shareToolAllowed(withRead, "list_projects", uuid.Nil); !ok {
		t.Fatal("list_projects with at least one read grant must be allowed")
	}

	emptyGrants := &auth.AuthContext{
		UserID:       uuid.New(),
		ShareTokenID: &projectA,
		ShareGrants:  nil,
	}
	if ok, _ := shareToolAllowed(emptyGrants, "list_projects", uuid.Nil); ok {
		t.Fatal("list_projects with zero grants must be rejected")
	}
}
