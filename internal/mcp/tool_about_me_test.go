package mcp

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

func TestAboutMe_Registered(t *testing.T) {
	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		deps := Dependencies{Backend: backend}
		srv := newTestServer(deps)
		tools := srv.MCPServer().ListTools()
		st, ok := tools["about_me"]
		if !ok {
			t.Fatalf("backend %s: about_me tool not registered", backend)
		}
		if len(st.Tool.RawOutputSchema) == 0 {
			t.Errorf("backend %s: about_me missing output schema", backend)
		}
	}
}

func TestHandleAboutMe_RejectsShareBearer(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	ctx := buildShareAuthCtx(uuid.New())
	result, err := handleAboutMe(ctx, srv, mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "not accessible to share-token")
}

func TestHandleAboutMe_NoAuth(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)

	result, err := handleAboutMe(buildNoAuthCtx(), srv, mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertToolError(t, result, "authentication required")
}

func TestHandleAboutMe_ReturnsFramingOrderedPersona(t *testing.T) {
	userID := uuid.New()
	userNS := uuid.New()
	aboutNS := uuid.New()

	user := &model.User{ID: userID, NamespaceID: userNS}
	aboutProject := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      aboutNS,
		OwnerNamespaceID: userNS,
		Slug:             model.ReservedProjectSlugAboutMe,
		Name:             "about_me",
	}

	// The lister returns memories already in framing order; the handler must
	// preserve that order and stamp every item with the about_me slug.
	m1, m2 := uuid.New(), uuid.New()
	lister := &mockMemoryListerByNs{
		memoriesByNs: map[uuid.UUID][]model.Memory{
			aboutNS: {
				{ID: m1, NamespaceID: aboutNS, Content: "most defining"},
				{ID: m2, NamespaceID: aboutNS, Content: "less defining"},
			},
		},
		countByNs: map[uuid.UUID]int{aboutNS: 2},
	}

	deps := Dependencies{
		Backend:      storage.BackendSQLite,
		UserRepo:     &mockUserRepoStore{user: user},
		ProjectRepo:  &mockProjectRepoStore{project: aboutProject},
		MemoryLister: lister,
	}
	srv := newTestServer(deps)

	result, err := handleAboutMe(buildAuthCtx(userID), srv, mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %v", result.Content)
	}

	var resp listMemoryResponse
	if err := json.Unmarshal([]byte(extractText(result)), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Pagination.Total != 2 {
		t.Errorf("expected total 2, got %d", resp.Pagination.Total)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(resp.Data))
	}
	if resp.Data[0].ID != m1 || resp.Data[1].ID != m2 {
		t.Errorf("framing order not preserved: got %v then %v", resp.Data[0].ID, resp.Data[1].ID)
	}
	for _, it := range resp.Data {
		if it.ProjectSlug != model.ReservedProjectSlugAboutMe {
			t.Errorf("entry %s stamped with %q, want about_me", it.ID, it.ProjectSlug)
		}
	}
}
