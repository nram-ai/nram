package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// --- mock DashboardStore ---

type mockDashboardStore struct {
	stats       *DashboardStatsData
	statsErr    error
	events      []ActivityEvent
	activityErr error
	lastLimit   int
	lastOrgID   *uuid.UUID
	lastUserID  *uuid.UUID
}

func (m *mockDashboardStore) DashboardStats(_ context.Context, orgID, userID *uuid.UUID) (*DashboardStatsData, error) {
	m.lastOrgID = orgID
	m.lastUserID = userID
	return m.stats, m.statsErr
}

func (m *mockDashboardStore) RecentActivity(_ context.Context, limit int, orgID, userID *uuid.UUID) ([]ActivityEvent, error) {
	m.lastLimit = limit
	m.lastOrgID = orgID
	m.lastUserID = userID
	return m.events, m.activityErr
}

// --- dashboard tests ---

func TestDashboardReturnsStats(t *testing.T) {
	projID := uuid.New()
	store := &mockDashboardStore{
		stats: &DashboardStatsData{
			TotalMemories: 42,
			TotalProjects: 3,
			TotalUsers:    5,
			TotalOrgs:     2,
			MemoriesByProject: []ProjectMemoryCount{
				{ProjectID: projID, ProjectName: "alpha", Count: 30},
			},
		},
	}

	h := NewAdminDashboardHandler(DashboardConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/dashboard", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp DashboardStatsData
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.TotalMemories != 42 {
		t.Errorf("expected total_memories 42, got %d", resp.TotalMemories)
	}
	if resp.TotalProjects != 3 {
		t.Errorf("expected total_projects 3, got %d", resp.TotalProjects)
	}
	if resp.TotalUsers != 5 {
		t.Errorf("expected total_users 5, got %d", resp.TotalUsers)
	}
	if resp.TotalOrgs != 2 {
		t.Errorf("expected total_organizations 2, got %d", resp.TotalOrgs)
	}
	if len(resp.MemoriesByProject) != 1 {
		t.Fatalf("expected 1 project, got %d", len(resp.MemoriesByProject))
	}
	if resp.MemoriesByProject[0].ProjectID != projID {
		t.Errorf("expected project_id %s, got %s", projID, resp.MemoriesByProject[0].ProjectID)
	}
	if resp.MemoriesByProject[0].ProjectName != "alpha" {
		t.Errorf("expected project_name alpha, got %q", resp.MemoriesByProject[0].ProjectName)
	}
	if resp.MemoriesByProject[0].Count != 30 {
		t.Errorf("expected count 30, got %d", resp.MemoriesByProject[0].Count)
	}
	if resp.EnrichmentQueue != nil {
		t.Error("expected enrichment_queue to be omitted")
	}
}

func TestDashboardWithEnrichmentQueue(t *testing.T) {
	store := &mockDashboardStore{
		stats: &DashboardStatsData{
			TotalMemories:     10,
			TotalProjects:     1,
			TotalUsers:        1,
			TotalOrgs:         1,
			MemoriesByProject: []ProjectMemoryCount{},
			EnrichmentQueue: &DashboardQueueStats{
				Pending:    5,
				Processing: 2,
				Failed:     1,
			},
		},
	}

	h := NewAdminDashboardHandler(DashboardConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/dashboard", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp DashboardStatsData
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.EnrichmentQueue == nil {
		t.Fatal("expected enrichment_queue to be present")
	}
	if resp.EnrichmentQueue.Pending != 5 {
		t.Errorf("expected pending 5, got %d", resp.EnrichmentQueue.Pending)
	}
	if resp.EnrichmentQueue.Processing != 2 {
		t.Errorf("expected processing 2, got %d", resp.EnrichmentQueue.Processing)
	}
	if resp.EnrichmentQueue.Failed != 1 {
		t.Errorf("expected failed 1, got %d", resp.EnrichmentQueue.Failed)
	}
}

func TestDashboardStoreError(t *testing.T) {
	store := &mockDashboardStore{
		statsErr: errors.New("db down"),
	}

	h := NewAdminDashboardHandler(DashboardConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/dashboard", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "internal_error" {
		t.Errorf("expected internal_error, got %q", resp.Error.Code)
	}
}

// --- activity tests ---

func TestActivityReturnsEvents(t *testing.T) {
	projID := uuid.New()
	userID := uuid.New()
	ts := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)

	store := &mockDashboardStore{
		events: []ActivityEvent{
			{
				ID:          "evt-1",
				Type:        "store",
				LengthChars: 17,
				ProjectID:   &projID,
				UserID:      &userID,
				Timestamp:   ts,
			},
			{
				ID:          "evt-2",
				Type:        "recall",
				LengthChars: 0,
				Timestamp:   ts.Add(-time.Minute),
			},
		},
	}

	h := NewAdminActivityHandler(DashboardConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/activity", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Events []ActivityEvent `json:"events"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(resp.Events))
	}
	if resp.Events[0].ID != "evt-1" {
		t.Errorf("expected id evt-1, got %q", resp.Events[0].ID)
	}
	if resp.Events[0].Type != "store" {
		t.Errorf("expected type store, got %q", resp.Events[0].Type)
	}
	if resp.Events[0].ProjectID == nil || *resp.Events[0].ProjectID != projID {
		t.Errorf("expected project_id %s", projID)
	}
	if resp.Events[1].ProjectID != nil {
		t.Error("expected second event project_id to be nil")
	}

	// Default limit should be 20.
	if store.lastLimit != 20 {
		t.Errorf("expected default limit 20, got %d", store.lastLimit)
	}
}

func TestActivityCustomLimit(t *testing.T) {
	store := &mockDashboardStore{
		events: []ActivityEvent{},
	}

	h := NewAdminActivityHandler(DashboardConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/activity?limit=5", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.lastLimit != 5 {
		t.Errorf("expected limit 5, got %d", store.lastLimit)
	}
}

func TestActivityInvalidLimitFallsBackToDefault(t *testing.T) {
	store := &mockDashboardStore{
		events: []ActivityEvent{},
	}

	h := NewAdminActivityHandler(DashboardConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/activity?limit=abc", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.lastLimit != 20 {
		t.Errorf("expected default limit 20, got %d", store.lastLimit)
	}
}

func TestActivityStoreError(t *testing.T) {
	store := &mockDashboardStore{
		activityErr: errors.New("query failed"),
	}

	h := NewAdminActivityHandler(DashboardConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/activity", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "internal_error" {
		t.Errorf("expected internal_error, got %q", resp.Error.Code)
	}
}

func TestActivityLimitCappedAt100(t *testing.T) {
	store := &mockDashboardStore{
		events: []ActivityEvent{},
	}

	h := NewAdminActivityHandler(DashboardConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/activity?limit=500", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.lastLimit != 100 {
		t.Errorf("expected limit capped to 100, got %d", store.lastLimit)
	}
}

// TestDashboardRoleTiers exercises the scope rule on /v1/dashboard and
// TestDashboardSelfScope verifies the post-fix tier-A semantics: /v1/dashboard
// and /v1/activity always pin to the caller's own scope. Widening attempts
// via ?org= or URL-path org_id are ignored regardless of role. Cross-tenant
// dashboards moved to /v1/admin/system/* and /v1/orgs/{id}/*.
func TestDashboardSelfScope(t *testing.T) {
	adminOrg := uuid.New()
	otherOrg := uuid.New()
	adminUser := uuid.New()

	cases := []struct {
		name     string
		auth     *auth.AuthContext
		query    string
		urlOrgID string
	}{
		{name: "admin alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}},
		{name: "admin ?org=other ignored", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}, query: "org=" + otherOrg.String()},
		{name: "admin URL path ignored", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}, urlOrgID: otherOrg.String()},
		{name: "org_owner alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleOrgOwner}},
		{name: "member alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleMember}},
	}

	for _, tc := range cases {
		t.Run("dashboard/"+tc.name, func(t *testing.T) {
			store := &mockDashboardStore{stats: &DashboardStatsData{}}
			h := NewAdminDashboardHandler(DashboardConfig{Store: store})

			url := "/v1/dashboard"
			if tc.query != "" {
				url += "?" + tc.query
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			ctx := auth.WithContext(req.Context(), tc.auth)
			if tc.urlOrgID != "" {
				rctx := chi.NewRouteContext()
				rctx.URLParams.Add("org_id", tc.urlOrgID)
				ctx = context.WithValue(ctx, chi.RouteCtxKey, rctx)
			}
			req = req.WithContext(ctx)

			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", w.Code)
			}

			if store.lastOrgID == nil || *store.lastOrgID != tc.auth.OrgID {
				t.Errorf("expected OrgID = caller's own org %v, got %v", tc.auth.OrgID, store.lastOrgID)
			}
		})

		t.Run("activity/"+tc.name, func(t *testing.T) {
			store := &mockDashboardStore{events: []ActivityEvent{}}
			h := NewAdminActivityHandler(DashboardConfig{Store: store})

			url := "/v1/activity"
			if tc.query != "" {
				url += "?" + tc.query
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			ctx := auth.WithContext(req.Context(), tc.auth)
			if tc.urlOrgID != "" {
				rctx := chi.NewRouteContext()
				rctx.URLParams.Add("org_id", tc.urlOrgID)
				ctx = context.WithValue(ctx, chi.RouteCtxKey, rctx)
			}
			req = req.WithContext(ctx)

			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", w.Code)
			}

			if store.lastOrgID == nil || *store.lastOrgID != tc.auth.OrgID {
				t.Errorf("expected OrgID = caller's own org %v, got %v", tc.auth.OrgID, store.lastOrgID)
			}
		})
	}
}
