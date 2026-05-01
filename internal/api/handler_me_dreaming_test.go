package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

type mockMeDreamStore struct {
	selfDirtyCount int
	selfDirtyErr   error
}

func (m *mockMeDreamStore) Status(_ context.Context) (*DreamStatusResponse, error) {
	return &DreamStatusResponse{}, nil
}
func (m *mockMeDreamStore) ProjectStatus(_ context.Context, _ uuid.UUID) (*DreamProjectStatusResponse, error) {
	return &DreamProjectStatusResponse{}, nil
}
func (m *mockMeDreamStore) ListCycles(_ context.Context, _ *uuid.UUID, _ int) ([]model.DreamCycle, error) {
	return nil, nil
}
func (m *mockMeDreamStore) ListSelfCycles(_ context.Context, _ *model.Namespace, _ int) ([]model.DreamCycle, error) {
	return nil, nil
}
func (m *mockMeDreamStore) SelfDreamingDirtyCount(_ context.Context, _ *model.Namespace) (int, error) {
	return m.selfDirtyCount, m.selfDirtyErr
}
func (m *mockMeDreamStore) GetCycle(_ context.Context, _ uuid.UUID) (*model.DreamCycle, error) {
	return &model.DreamCycle{}, nil
}
func (m *mockMeDreamStore) GetCycleLogs(_ context.Context, _ uuid.UUID) ([]model.DreamLog, error) {
	return nil, nil
}
func (m *mockMeDreamStore) SetEnabled(_ context.Context, _ bool) error                      { return nil }
func (m *mockMeDreamStore) AbandonCycle(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return true, nil
}

type mockMeDreamProjects struct {
	countByUser   int
	countByUserFn func(ownerNSID uuid.UUID) (int, error)
	getByIDFn     func(id uuid.UUID) (*model.Project, error)
}

func (m *mockMeDreamProjects) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(id)
	}
	return &model.Project{ID: id, NamespaceID: uuid.New()}, nil
}

func (m *mockMeDreamProjects) CountByUser(_ context.Context, ownerNSID uuid.UUID) (int, error) {
	if m.countByUserFn != nil {
		return m.countByUserFn(ownerNSID)
	}
	return m.countByUser, nil
}

// callerNamespacePath is the path the namespace mock returns for the caller's
// own namespace; project namespaces returned for ownership checks live under
// this prefix unless a test overrides them.
const callerNamespacePath = "users/alice"

type mockMeDreamNamespaces struct {
	getFn func(id uuid.UUID) (*model.Namespace, error)
}

func (m *mockMeDreamNamespaces) GetByID(_ context.Context, id uuid.UUID) (*model.Namespace, error) {
	if m.getFn != nil {
		return m.getFn(id)
	}
	return &model.Namespace{ID: id, Path: callerNamespacePath, Depth: 1}, nil
}

func newMeDreamRequest(t *testing.T, target string, ac *auth.AuthContext) (*http.Request, *httptest.ResponseRecorder) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	if ac != nil {
		r = r.WithContext(auth.WithContext(r.Context(), ac))
	}
	return r, httptest.NewRecorder()
}

func TestMeDreaming_AggregateStatus_ExposesDirtyCount(t *testing.T) {
	store := &mockMeDreamStore{selfDirtyCount: 4}
	projects := &mockMeDreamProjects{countByUser: 7}
	user := &model.User{ID: uuid.New(), NamespaceID: uuid.New()}

	h := NewSelfDreamingHandler(MeDreamingConfig{
		Store:      store,
		Projects:   projects,
		Namespaces: &mockMeDreamNamespaces{},
		Users:      &mockUserGetter{user: user},
	})

	r, w := newMeDreamRequest(t, "/v1/me/dreaming", &auth.AuthContext{UserID: user.ID, Role: "user"})
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	// Decode into a generic map so we can assert on raw keys — guards
	// against a regression that re-introduces the old `any_dirty` bool.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, present := raw["any_dirty"]; present {
		t.Fatalf("response must not include legacy any_dirty key, body: %s", w.Body.String())
	}

	var body MeDreamingAggregateStatus
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode into struct: %v", err)
	}
	if body.DirtyCount != 4 {
		t.Fatalf("expected dirty_count=4, got %d", body.DirtyCount)
	}
	if body.ProjectCount != 7 {
		t.Fatalf("expected project_count=7, got %d", body.ProjectCount)
	}
}

func TestMeDreaming_AggregateStatus_ZeroDirty(t *testing.T) {
	store := &mockMeDreamStore{selfDirtyCount: 0}
	projects := &mockMeDreamProjects{countByUser: 3}
	user := &model.User{ID: uuid.New(), NamespaceID: uuid.New()}

	h := NewSelfDreamingHandler(MeDreamingConfig{
		Store:      store,
		Projects:   projects,
		Namespaces: &mockMeDreamNamespaces{},
		Users:      &mockUserGetter{user: user},
	})

	r, w := newMeDreamRequest(t, "/v1/me/dreaming", &auth.AuthContext{UserID: user.ID, Role: "user"})
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	var body MeDreamingAggregateStatus
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.DirtyCount != 0 {
		t.Fatalf("expected dirty_count=0, got %d", body.DirtyCount)
	}
	if body.ProjectCount != 3 {
		t.Fatalf("expected project_count=3, got %d", body.ProjectCount)
	}
}

func TestMeDreaming_AggregateStatus_RequiresAuth(t *testing.T) {
	h := NewSelfDreamingHandler(MeDreamingConfig{
		Store:      &mockMeDreamStore{},
		Projects:   &mockMeDreamProjects{},
		Namespaces: &mockMeDreamNamespaces{},
		Users:      &mockUserGetter{},
	})

	r, w := newMeDreamRequest(t, "/v1/me/dreaming", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", w.Code)
	}
}

func TestMeDreaming_PerProject_ForbiddenWhenNotOwned(t *testing.T) {
	user := &model.User{ID: uuid.New(), NamespaceID: uuid.New()}
	otherNSID := uuid.New()
	projectID := uuid.New()

	projects := &mockMeDreamProjects{
		getByIDFn: func(id uuid.UUID) (*model.Project, error) {
			return &model.Project{ID: id, NamespaceID: otherNSID}, nil
		},
	}
	namespaces := &mockMeDreamNamespaces{
		getFn: func(id uuid.UUID) (*model.Namespace, error) {
			// Caller's namespace returns the canonical caller path; the
			// project's namespace returns an unrelated path so the
			// ownership check fails.
			if id == otherNSID {
				return &model.Namespace{ID: id, Path: "users/bob"}, nil
			}
			return &model.Namespace{ID: id, Path: callerNamespacePath}, nil
		},
	}

	h := NewSelfDreamingHandler(MeDreamingConfig{
		Store:      &mockMeDreamStore{},
		Projects:   projects,
		Namespaces: namespaces,
		Users:      &mockUserGetter{user: user},
	})

	r, w := newMeDreamRequest(t, "/v1/me/dreaming?project_id="+projectID.String(), &auth.AuthContext{UserID: user.ID, Role: "user"})
	h.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-owned project, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestMeDreaming_PerProject_BadProjectID(t *testing.T) {
	user := &model.User{ID: uuid.New(), NamespaceID: uuid.New()}

	h := NewSelfDreamingHandler(MeDreamingConfig{
		Store:      &mockMeDreamStore{},
		Projects:   &mockMeDreamProjects{},
		Namespaces: &mockMeDreamNamespaces{},
		Users:      &mockUserGetter{user: user},
	})

	r, w := newMeDreamRequest(t, "/v1/me/dreaming?project_id=not-a-uuid", &auth.AuthContext{UserID: user.ID, Role: "user"})
	h.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid project_id, got %d", w.Code)
	}
}
