package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// memProceduralRepo is an in-memory ProceduralRepository for MCP handler tests.
type memProceduralRepo struct {
	rows map[uuid.UUID]*model.ProceduralEntry
}

func newMemProceduralRepo() *memProceduralRepo {
	return &memProceduralRepo{rows: map[uuid.UUID]*model.ProceduralEntry{}}
}

func (m *memProceduralRepo) Create(_ context.Context, e *model.ProceduralEntry) error {
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	cp := *e
	m.rows[e.ID] = &cp
	return nil
}

func (m *memProceduralRepo) GetByID(_ context.Context, id uuid.UUID) (*model.ProceduralEntry, error) {
	e, ok := m.rows[id]
	if !ok {
		return nil, sql.ErrNoRows
	}
	cp := *e
	return &cp, nil
}

func (m *memProceduralRepo) ListByNamespace(_ context.Context, ns uuid.UUID) ([]model.ProceduralEntry, error) {
	out := []model.ProceduralEntry{}
	for _, e := range m.rows {
		if e.NamespaceID == ns {
			out = append(out, *e)
		}
	}
	// Order by priority DESC for deterministic assertions.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Priority > out[i].Priority {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

func (m *memProceduralRepo) CountByNamespace(_ context.Context, ns uuid.UUID) (int, error) {
	n := 0
	for _, e := range m.rows {
		if e.NamespaceID == ns {
			n++
		}
	}
	return n, nil
}

func (m *memProceduralRepo) Update(_ context.Context, e *model.ProceduralEntry) error {
	existing, ok := m.rows[e.ID]
	if !ok || existing.NamespaceID != e.NamespaceID {
		return sql.ErrNoRows
	}
	cp := *e
	m.rows[e.ID] = &cp
	return nil
}

func (m *memProceduralRepo) Delete(_ context.Context, id, ns uuid.UUID) error {
	e, ok := m.rows[id]
	if !ok || e.NamespaceID != ns {
		return sql.ErrNoRows
	}
	delete(m.rows, id)
	return nil
}

func buildShareAuthCtx(userID uuid.UUID) context.Context {
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	shareID := uuid.New()
	ac := &auth.AuthContext{UserID: userID, ShareTokenID: &shareID}
	req = req.WithContext(auth.WithContext(req.Context(), ac))
	return context.WithValue(context.Background(), httpRequestKey, req)
}

func newProceduralTestServer(userID, nsID uuid.UUID) (*Server, *memProceduralRepo) {
	repo := newMemProceduralRepo()
	deps := Dependencies{
		UserRepo:   &mockUserRepoStore{user: &model.User{ID: userID, NamespaceID: nsID}},
		Procedural: service.NewProceduralService(repo),
	}
	return newTestServer(deps), repo
}

func callProcedural(t *testing.T, fn func(context.Context, *Server, mcp.CallToolRequest) (*mcp.CallToolResult, error),
	ctx context.Context, s *Server, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	res, err := fn(ctx, s, req)
	if err != nil {
		t.Fatalf("%s: unexpected Go error: %v", name, err)
	}
	return res
}

func TestProceduralMCP_RoundTrip(t *testing.T) {
	userID, nsID := uuid.New(), uuid.New()
	srv, _ := newProceduralTestServer(userID, nsID)
	ctx := buildAuthCtx(userID)

	// Store two entries: one enabled (priority 5), one disabled (priority 9).
	enabledRes := callProcedural(t, handleProceduralStore, ctx, srv, "procedural_store", map[string]any{
		"content":  "Always verify before claiming done.",
		"title":    "verify",
		"priority": float64(5),
	})
	if enabledRes.IsError {
		t.Fatalf("store enabled: %s", extractText(enabledRes))
	}
	var enabled mcpProceduralEntry
	if err := json.Unmarshal([]byte(extractText(enabledRes)), &enabled); err != nil {
		t.Fatalf("unmarshal enabled: %v", err)
	}
	if !enabled.Enabled {
		t.Fatal("expected enabled=true by default")
	}

	disabledRes := callProcedural(t, handleProceduralStore, ctx, srv, "procedural_store", map[string]any{
		"content":  "Parked rule.",
		"priority": float64(9),
		"enabled":  false,
	})
	if disabledRes.IsError {
		t.Fatalf("store disabled: %s", extractText(disabledRes))
	}

	// Fetch returns ONLY the enabled entry, despite the disabled one having
	// higher priority.
	fetchRes := callProcedural(t, handleProceduralFetch, ctx, srv, "procedural_fetch", map[string]any{})
	if fetchRes.IsError {
		t.Fatalf("fetch: %s", extractText(fetchRes))
	}
	var fetch mcpProceduralFetchResponse
	if err := json.Unmarshal([]byte(extractText(fetchRes)), &fetch); err != nil {
		t.Fatalf("unmarshal fetch: %v", err)
	}
	if fetch.Count != 1 || len(fetch.Entries) != 1 {
		t.Fatalf("expected 1 enabled entry, got %d", fetch.Count)
	}
	if fetch.Entries[0].Content != "Always verify before claiming done." {
		t.Fatalf("unexpected fetched content: %q", fetch.Entries[0].Content)
	}

	// Update: enable the parked entry and bump priority via the enabled one's
	// id is not needed; toggle the disabled entry on.
	var disabled mcpProceduralEntry
	_ = json.Unmarshal([]byte(extractText(disabledRes)), &disabled)
	updRes := callProcedural(t, handleProceduralUpdate, ctx, srv, "procedural_update", map[string]any{
		"id":      disabled.ID.String(),
		"enabled": true,
	})
	if updRes.IsError {
		t.Fatalf("update: %s", extractText(updRes))
	}

	// Now fetch returns both, ordered by priority (9 before 5).
	fetchRes2 := callProcedural(t, handleProceduralFetch, ctx, srv, "procedural_fetch", map[string]any{})
	var fetch2 mcpProceduralFetchResponse
	_ = json.Unmarshal([]byte(extractText(fetchRes2)), &fetch2)
	if fetch2.Count != 2 {
		t.Fatalf("expected 2 enabled entries after toggle, got %d", fetch2.Count)
	}
	if fetch2.Entries[0].Priority < fetch2.Entries[1].Priority {
		t.Fatalf("expected priority-desc ordering, got %d then %d", fetch2.Entries[0].Priority, fetch2.Entries[1].Priority)
	}

	// Forget the enabled entry.
	forgetRes := callProcedural(t, handleProceduralForget, ctx, srv, "procedural_forget", map[string]any{
		"id": enabled.ID.String(),
	})
	if forgetRes.IsError {
		t.Fatalf("forget: %s", extractText(forgetRes))
	}
	fetchRes3 := callProcedural(t, handleProceduralFetch, ctx, srv, "procedural_fetch", map[string]any{})
	var fetch3 mcpProceduralFetchResponse
	_ = json.Unmarshal([]byte(extractText(fetchRes3)), &fetch3)
	if fetch3.Count != 1 {
		t.Fatalf("expected 1 entry after forget, got %d", fetch3.Count)
	}
}

func TestProceduralMCP_EmptyContentRejected(t *testing.T) {
	userID, nsID := uuid.New(), uuid.New()
	srv, _ := newProceduralTestServer(userID, nsID)
	ctx := buildAuthCtx(userID)

	res := callProcedural(t, handleProceduralStore, ctx, srv, "procedural_store", map[string]any{
		"content": "   ",
	})
	if !res.IsError {
		t.Fatal("expected error for blank content")
	}
}

func TestProceduralMCP_ShareBearerRejected(t *testing.T) {
	userID, nsID := uuid.New(), uuid.New()
	srv, _ := newProceduralTestServer(userID, nsID)
	ctx := buildShareAuthCtx(userID)

	for _, tc := range []struct {
		name string
		fn   func(context.Context, *Server, mcp.CallToolRequest) (*mcp.CallToolResult, error)
	}{
		{"procedural_fetch", handleProceduralFetch},
		{"procedural_store", handleProceduralStore},
		{"procedural_update", handleProceduralUpdate},
		{"procedural_forget", handleProceduralForget},
	} {
		res := callProcedural(t, tc.fn, ctx, srv, tc.name, map[string]any{"content": "x", "id": uuid.New().String()})
		if !res.IsError {
			t.Fatalf("%s: expected share-bearer to be rejected", tc.name)
		}
	}
}

func TestProceduralMCP_CrossNamespaceUpdateBlocked(t *testing.T) {
	userID, nsID := uuid.New(), uuid.New()
	srv, repo := newProceduralTestServer(userID, nsID)
	ctx := buildAuthCtx(userID)

	// Seed an entry owned by a DIFFERENT namespace.
	foreign := &model.ProceduralEntry{ID: uuid.New(), NamespaceID: uuid.New(), Content: "foreign", Enabled: true}
	_ = repo.Create(ctx, foreign)

	res := callProcedural(t, handleProceduralUpdate, ctx, srv, "procedural_update", map[string]any{
		"id":      foreign.ID.String(),
		"content": "hijack",
	})
	if !res.IsError {
		t.Fatal("expected cross-namespace update to be rejected as not found")
	}
}
