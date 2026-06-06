package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// mockOrgAnalyticsStore implements OrgAnalyticsAggregator for handler tests.
type mockOrgAnalyticsStore struct {
	memoryCounts  MemoryCountsData
	recall        []HistogramBucket
	users         []UserAggregate
	entities      []TypeBucket
	relationships []TypeBucket
}

func (m *mockOrgAnalyticsStore) OrgMemoryCounts(_ context.Context, _ uuid.UUID) (MemoryCountsData, error) {
	return m.memoryCounts, nil
}
func (m *mockOrgAnalyticsStore) RecallDistribution(_ context.Context, _ *uuid.UUID) ([]HistogramBucket, error) {
	return m.recall, nil
}
func (m *mockOrgAnalyticsStore) UserBreakdown(_ context.Context, _ uuid.UUID) ([]UserAggregate, error) {
	return m.users, nil
}
func (m *mockOrgAnalyticsStore) EntityTypeHistogram(_ context.Context, _ *uuid.UUID) ([]TypeBucket, error) {
	return m.entities, nil
}
func (m *mockOrgAnalyticsStore) RelationshipTypeHistogram(_ context.Context, _ *uuid.UUID) ([]TypeBucket, error) {
	return m.relationships, nil
}

// orgAnalyticsRequest builds a chi-routed request that has {org_id} in the
// URL params, since OrgScope reads via chi.URLParam.
func orgAnalyticsRequest(t *testing.T, h http.HandlerFunc, orgID uuid.UUID, ac *auth.AuthContext) *httptest.ResponseRecorder {
	t.Helper()
	r := chi.NewRouter()
	r.Get("/v1/orgs/{org_id}/analytics", h)

	req := httptest.NewRequest(http.MethodGet, "/v1/orgs/"+orgID.String()+"/analytics", nil)
	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestOrgAnalytics_ReturnsUserBreakdownNotProjects(t *testing.T) {
	orgID := uuid.New()
	store := &mockOrgAnalyticsStore{
		memoryCounts: MemoryCountsData{Total: 7, Active: 5},
		users: []UserAggregate{
			{UserID: uuid.New(), Email: "alice@example.com", TotalMemories: 4, TotalProjects: 2, TotalEntities: 3},
			{UserID: uuid.New(), Email: "bob@example.com", TotalMemories: 1, TotalProjects: 1, TotalEntities: 0},
		},
	}
	h := NewOrgAnalyticsHandler(OrgAnalyticsConfig{Store: store})

	ac := &auth.AuthContext{
		UserID: uuid.New(),
		OrgID:  orgID,
		Role:   auth.RoleOrgOwner,
	}
	w := orgAnalyticsRequest(t, h, orgID, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Decode into a generic map so we can prove the wire-format key is
	// user_breakdown (not project_breakdown, which was the privacy leak).
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := raw["project_breakdown"]; ok {
		t.Fatal("response must not include project_breakdown: leaks per-user activity to org_owners")
	}
	users, ok := raw["user_breakdown"]
	if !ok {
		t.Fatal("response missing user_breakdown")
	}

	var got []UserAggregate
	if err := json.Unmarshal(users, &got); err != nil {
		t.Fatalf("decode user_breakdown: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 user rows, got %d", len(got))
	}
	if got[0].Email != "alice@example.com" || got[0].TotalMemories != 4 {
		t.Errorf("first row mismatch: %+v", got[0])
	}
}

func TestOrgAnalytics_RejectsMember(t *testing.T) {
	orgID := uuid.New()
	store := &mockOrgAnalyticsStore{}
	h := NewOrgAnalyticsHandler(OrgAnalyticsConfig{Store: store})

	// Plain member of the org; OrgAccessMiddleware admits them, but the
	// handler must enforce requireOrgOwner. Aggregate views are owner-only.
	ac := &auth.AuthContext{
		UserID: uuid.New(),
		OrgID:  orgID,
		Role:   auth.RoleMember,
	}
	w := orgAnalyticsRequest(t, h, orgID, ac)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
}
