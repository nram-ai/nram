package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// mockDreamAdminStore implements DreamAdminStore for handler tests.
type mockDreamAdminStore struct {
	abandonOK     bool
	abandonErr    error
	lastCycleID   uuid.UUID
	lastReason    string
}

func (m *mockDreamAdminStore) Status(_ context.Context) (*DreamStatusResponse, error) {
	return &DreamStatusResponse{}, nil
}
func (m *mockDreamAdminStore) ProjectStatus(_ context.Context, _ uuid.UUID) (*DreamProjectStatusResponse, error) {
	return &DreamProjectStatusResponse{}, nil
}
func (m *mockDreamAdminStore) ListCycles(_ context.Context, _ *uuid.UUID, _ int) ([]model.DreamCycle, error) {
	return nil, nil
}
func (m *mockDreamAdminStore) GetCycle(_ context.Context, _ uuid.UUID) (*model.DreamCycle, error) {
	return &model.DreamCycle{}, nil
}
func (m *mockDreamAdminStore) GetCycleLogs(_ context.Context, _ uuid.UUID) ([]model.DreamLog, error) {
	return nil, nil
}
func (m *mockDreamAdminStore) SetEnabled(_ context.Context, _ bool) error              { return nil }
func (m *mockDreamAdminStore) SetProjectEnabled(_ context.Context, _ uuid.UUID, _ bool) error {
	return nil
}
func (m *mockDreamAdminStore) AbandonCycle(_ context.Context, id uuid.UUID, reason string) (bool, error) {
	m.lastCycleID = id
	m.lastReason = reason
	return m.abandonOK, m.abandonErr
}

type mockDreamRollbacker struct{}

func (m *mockDreamRollbacker) Rollback(_ context.Context, _ uuid.UUID) error { return nil }

func newAbandonRequest(t *testing.T, cycleIDStr string, role string) (*http.Request, *httptest.ResponseRecorder) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/dreaming/cycles/"+cycleIDStr+"/abandon", nil)
	if role != "" {
		ac := &auth.AuthContext{Role: role}
		r = r.WithContext(auth.WithContext(r.Context(), ac))
	}
	return r, httptest.NewRecorder()
}

func TestAbandonHandler_RequiresAdministrator(t *testing.T) {
	store := &mockDreamAdminStore{abandonOK: true}
	h := NewAdminDreamingHandler(DreamAdminConfig{Store: store, Rollback: &mockDreamRollbacker{}})

	cycleID := uuid.New().String()

	cases := []struct {
		name     string
		role     string
		wantCode int
	}{
		{"no auth", "", http.StatusForbidden},
		{"reader role", "reader", http.StatusForbidden},
		{"administrator role", auth.RoleAdministrator, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, w := newAbandonRequest(t, cycleID, tc.role)
			h.ServeHTTP(w, r)
			if w.Code != tc.wantCode {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.wantCode, w.Code, w.Body.String())
			}
		})
	}
}

func TestAbandonHandler_BadCycleID(t *testing.T) {
	store := &mockDreamAdminStore{}
	h := NewAdminDreamingHandler(DreamAdminConfig{Store: store, Rollback: &mockDreamRollbacker{}})

	r, w := newAbandonRequest(t, "not-a-uuid", auth.RoleAdministrator)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid uuid, got %d", w.Code)
	}
}

func TestAbandonHandler_AlreadyTerminalReturnsConflict(t *testing.T) {
	store := &mockDreamAdminStore{abandonOK: false}
	h := NewAdminDreamingHandler(DreamAdminConfig{Store: store, Rollback: &mockDreamRollbacker{}})

	cycleID := uuid.New().String()
	r, w := newAbandonRequest(t, cycleID, auth.RoleAdministrator)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 when cycle already terminal, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestAbandonHandler_StoreErrorReturns500(t *testing.T) {
	store := &mockDreamAdminStore{abandonErr: errors.New("boom")}
	h := NewAdminDreamingHandler(DreamAdminConfig{Store: store, Rollback: &mockDreamRollbacker{}})

	r, w := newAbandonRequest(t, uuid.New().String(), auth.RoleAdministrator)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on store error, got %d", w.Code)
	}
}

func TestAbandonHandler_SuccessShape(t *testing.T) {
	store := &mockDreamAdminStore{abandonOK: true}
	h := NewAdminDreamingHandler(DreamAdminConfig{Store: store, Rollback: &mockDreamRollbacker{}})

	cycleID := uuid.New()
	r, w := newAbandonRequest(t, cycleID.String(), auth.RoleAdministrator)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	var body struct {
		Status  string `json:"status"`
		CycleID string `json:"cycle_id"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "failed" {
		t.Fatalf("expected status=failed, got %q", body.Status)
	}
	if body.CycleID != cycleID.String() {
		t.Fatalf("expected cycle_id round-trip, got %q", body.CycleID)
	}
	if store.lastCycleID != cycleID {
		t.Fatalf("expected store called with cycle id %s, got %s", cycleID, store.lastCycleID)
	}
	if store.lastReason == "" {
		t.Fatalf("expected non-empty abandon reason")
	}
}

func TestAbandonHandler_OnlyPOST(t *testing.T) {
	store := &mockDreamAdminStore{abandonOK: true}
	h := NewAdminDreamingHandler(DreamAdminConfig{Store: store, Rollback: &mockDreamRollbacker{}})

	cycleID := uuid.New().String()
	r := httptest.NewRequest(http.MethodGet, "/v1/dreaming/cycles/"+cycleID+"/abandon", nil)
	r = r.WithContext(auth.WithContext(r.Context(), &auth.AuthContext{Role: auth.RoleAdministrator}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code == http.StatusOK {
		t.Fatalf("GET on abandon route should not return 200, got %d", w.Code)
	}
}
