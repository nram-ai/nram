package mcp

import (
	"testing"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// TestHandleProjectUpdate_RejectsReservedIdentityEdits verifies that the
// update_project tool refuses to change a reserved project's name or
// description (nram-managed), while leaving default_tags editable.
func TestHandleProjectUpdate_RejectsReservedIdentityEdits(t *testing.T) {
	userID := uuid.New()
	nsID := uuid.New()
	user := &model.User{ID: userID, NamespaceID: nsID}
	reserved := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      uuid.New(),
		OwnerNamespaceID: nsID,
		Slug:             model.ReservedProjectSlugAboutMe,
		Name:             "about_me",
	}

	deps := Dependencies{
		Backend:     storage.BackendSQLite,
		UserRepo:    &mockUserRepoStore{user: user},
		ProjectRepo: &mockProjectRepoStore{project: reserved},
	}
	srv := newTestServer(deps)
	ctx := buildAuthCtx(userID)

	mkReq := func(args map[string]any) mcp.CallToolRequest {
		req := mcp.CallToolRequest{}
		req.Params.Arguments = args
		return req
	}

	nameRes, err := handleProjectUpdate(ctx, srv, mkReq(map[string]any{"project": "about_me", "name": "Renamed"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, nameRes, "reserved")

	descRes, err := handleProjectUpdate(ctx, srv, mkReq(map[string]any{"project": "about_me", "description": "hijack"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, descRes, "reserved")
}
