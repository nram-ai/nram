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

// --- mock analytics store ---

type mockAnalyticsStore struct {
	data       *AnalyticsData
	err        error
	called     bool
	lastOrgID  *uuid.UUID
	lastUserID *uuid.UUID
}

func (m *mockAnalyticsStore) GetAnalytics(_ context.Context, orgID *uuid.UUID, userID *uuid.UUID) (*AnalyticsData, error) {
	m.called = true
	m.lastOrgID = orgID
	m.lastUserID = userID
	return m.data, m.err
}

// --- tests ---

func TestAdminAnalytics_Success(t *testing.T) {
	pid := uuid.New()
	now := time.Now().Truncate(time.Second)

	store := &mockAnalyticsStore{
		data: &AnalyticsData{
			MemoryCounts: MemoryCountsData{
				Total:    100,
				Active:   80,
				Deleted:  15,
				Enriched: 65,
			},
			MostRecalled: []MemoryRankItem{
				{
					ID:          uuid.New(),
					LengthChars: len("frequently recalled memory"),
					AccessCount: 42,
					ProjectID:   &pid,
					CreatedAt:   now,
				},
			},
			LeastRecalled: []MemoryRankItem{
				{
					ID:          uuid.New(),
					LengthChars: len("rarely recalled memory"),
					AccessCount: 1,
					CreatedAt:   now,
				},
			},
			DeadWeight: []MemoryRankItem{
				{
					ID:          uuid.New(),
					LengthChars: len("never accessed memory"),
					AccessCount: 0,
					CreatedAt:   now,
				},
			},
			EnrichmentStats: EnrichmentStatsData{
				TotalProcessed: 200,
				SuccessRate:    0.95,
				FailureRate:    0.05,
				AvgLatencyMs:   150,
			},
		},
	}

	h := NewAdminAnalyticsHandler(AnalyticsConfig{Store: store})

	req := tierAReq("/v1/admin/analytics")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp AnalyticsData
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.MemoryCounts.Total != 100 {
		t.Errorf("expected total 100, got %d", resp.MemoryCounts.Total)
	}
	if resp.MemoryCounts.Active != 80 {
		t.Errorf("expected active 80, got %d", resp.MemoryCounts.Active)
	}
	if resp.MemoryCounts.Deleted != 15 {
		t.Errorf("expected deleted 15, got %d", resp.MemoryCounts.Deleted)
	}
	if resp.MemoryCounts.Enriched != 65 {
		t.Errorf("expected enriched 65, got %d", resp.MemoryCounts.Enriched)
	}

	if len(resp.MostRecalled) != 1 {
		t.Fatalf("expected 1 most recalled, got %d", len(resp.MostRecalled))
	}
	if resp.MostRecalled[0].AccessCount != 42 {
		t.Errorf("expected access_count 42, got %d", resp.MostRecalled[0].AccessCount)
	}
	if resp.MostRecalled[0].LengthChars != len("frequently recalled memory") {
		t.Errorf("expected length_chars %d, got %d", len("frequently recalled memory"), resp.MostRecalled[0].LengthChars)
	}
	if resp.MostRecalled[0].ProjectID == nil {
		t.Error("expected project_id to be set on most recalled item")
	}

	if len(resp.LeastRecalled) != 1 {
		t.Fatalf("expected 1 least recalled, got %d", len(resp.LeastRecalled))
	}
	if resp.LeastRecalled[0].AccessCount != 1 {
		t.Errorf("expected access_count 1, got %d", resp.LeastRecalled[0].AccessCount)
	}
	if resp.LeastRecalled[0].ProjectID != nil {
		t.Error("expected project_id to be nil on least recalled item")
	}

	if len(resp.DeadWeight) != 1 {
		t.Fatalf("expected 1 dead weight, got %d", len(resp.DeadWeight))
	}
	if resp.DeadWeight[0].AccessCount != 0 {
		t.Errorf("expected access_count 0, got %d", resp.DeadWeight[0].AccessCount)
	}

	if resp.EnrichmentStats.TotalProcessed != 200 {
		t.Errorf("expected total_processed 200, got %d", resp.EnrichmentStats.TotalProcessed)
	}
	if resp.EnrichmentStats.SuccessRate != 0.95 {
		t.Errorf("expected success_rate 0.95, got %f", resp.EnrichmentStats.SuccessRate)
	}
	if resp.EnrichmentStats.FailureRate != 0.05 {
		t.Errorf("expected failure_rate 0.05, got %f", resp.EnrichmentStats.FailureRate)
	}
	if resp.EnrichmentStats.AvgLatencyMs != 150 {
		t.Errorf("expected avg_latency_ms 150, got %d", resp.EnrichmentStats.AvgLatencyMs)
	}
}

func TestAdminAnalytics_StoreError(t *testing.T) {
	store := &mockAnalyticsStore{
		err: errors.New("database unavailable"),
	}

	h := NewAdminAnalyticsHandler(AnalyticsConfig{Store: store})

	req := tierAReq("/v1/admin/analytics")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Error == nil {
		t.Fatal("expected error envelope")
	}
	if resp.Error.Code != "internal_error" {
		t.Errorf("expected code internal_error, got %q", resp.Error.Code)
	}
}

func TestAdminAnalytics_EmptyData(t *testing.T) {
	store := &mockAnalyticsStore{
		data: &AnalyticsData{
			MemoryCounts:    MemoryCountsData{},
			MostRecalled:    []MemoryRankItem{},
			LeastRecalled:   []MemoryRankItem{},
			DeadWeight:      []MemoryRankItem{},
			EnrichmentStats: EnrichmentStatsData{},
		},
	}

	h := NewAdminAnalyticsHandler(AnalyticsConfig{Store: store})

	req := tierAReq("/v1/admin/analytics")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp AnalyticsData
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.MemoryCounts.Total != 0 {
		t.Errorf("expected total 0, got %d", resp.MemoryCounts.Total)
	}
	if resp.MemoryCounts.Active != 0 {
		t.Errorf("expected active 0, got %d", resp.MemoryCounts.Active)
	}
	if len(resp.MostRecalled) != 0 {
		t.Errorf("expected 0 most recalled, got %d", len(resp.MostRecalled))
	}
	if len(resp.LeastRecalled) != 0 {
		t.Errorf("expected 0 least recalled, got %d", len(resp.LeastRecalled))
	}
	if len(resp.DeadWeight) != 0 {
		t.Errorf("expected 0 dead weight, got %d", len(resp.DeadWeight))
	}
	if resp.EnrichmentStats.TotalProcessed != 0 {
		t.Errorf("expected total_processed 0, got %d", resp.EnrichmentStats.TotalProcessed)
	}
	if resp.EnrichmentStats.SuccessRate != 0 {
		t.Errorf("expected success_rate 0, got %f", resp.EnrichmentStats.SuccessRate)
	}
}

func TestAdminAnalytics_WrongMethod(t *testing.T) {
	store := &mockAnalyticsStore{
		data: &AnalyticsData{},
	}

	h := NewAdminAnalyticsHandler(AnalyticsConfig{Store: store})

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/v1/admin/analytics", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", method, w.Code)
		}

		var resp errorEnvelope
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("%s: decode: %v", method, err)
		}

		if resp.Error == nil {
			t.Fatalf("%s: expected error envelope", method)
		}
		if resp.Error.Code != "bad_request" {
			t.Errorf("%s: expected code bad_request, got %q", method, resp.Error.Code)
		}
		if resp.Error.Message != "method not allowed" {
			t.Errorf("%s: expected message 'method not allowed', got %q", method, resp.Error.Message)
		}
	}
}

// TestAdminAnalytics_SelfScope verifies the post-fix tier-A semantics: the
// /v1/analytics handler always returns the caller's own scope and ignores
// any ?org= / ?user= widening attempt by any role, including administrator.
// The widening capability was removed in the 2026-04-30 leak fix; admin
// drilling moves to /v1/admin/system/analytics and /v1/orgs/{id}/analytics.
func TestAdminAnalytics_SelfScope(t *testing.T) {
	adminOrg := uuid.New()
	otherOrg := uuid.New()
	adminUser := uuid.New()
	otherUser := uuid.New()

	cases := []struct {
		name string
		auth *auth.AuthContext
		// query/urlOrgID are widening attempts that MUST be ignored.
		query    string
		urlOrgID string
	}{
		{name: "admin alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}},
		{name: "admin with ?org=other ignored", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}, query: "org=" + otherOrg.String()},
		{name: "admin with ?user=other ignored", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}, query: "user=" + otherUser.String()},
		{name: "admin URL path ignored", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}, urlOrgID: otherOrg.String()},
		{name: "org_owner alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleOrgOwner}},
		{name: "member alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleMember}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockAnalyticsStore{data: &AnalyticsData{}}
			h := NewAdminAnalyticsHandler(AnalyticsConfig{Store: store})

			url := "/v1/analytics"
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

			// Self-tier: must always pin to the caller's own org+user,
			// regardless of widening attempts.
			if store.lastOrgID == nil || *store.lastOrgID != tc.auth.OrgID {
				t.Errorf("expected OrgID = caller's org %v, got %v", tc.auth.OrgID, store.lastOrgID)
			}
			if store.lastUserID == nil || *store.lastUserID != tc.auth.UserID {
				t.Errorf("expected UserID = caller's user %v, got %v", tc.auth.UserID, store.lastUserID)
			}
		})
	}
}

// TestAdminAnalytics_NilOrgFailsClosed verifies that an org-less principal
// (OrgID == uuid.Nil, or a nil AuthContext) is rejected with 403 and never
// reaches the store. Before the fix, SelfScope returned (nil, nil) for such a
// caller and the store read that as "global", leaking system-wide, cross-tenant
// aggregates on the tier-A /v1/analytics route.
func TestAdminAnalytics_NilOrgFailsClosed(t *testing.T) {
	for _, tc := range nilOrgFailsClosedCases {
		t.Run(tc.name, func(t *testing.T) {
			// Sentinel data proves nothing leaks even if the store were reached.
			store := &mockAnalyticsStore{data: &AnalyticsData{MemoryCounts: MemoryCountsData{Total: 999}}}
			h := NewAdminAnalyticsHandler(AnalyticsConfig{Store: store})

			req := httptest.NewRequest(http.MethodGet, "/v1/analytics", nil)
			if tc.auth != nil {
				req = req.WithContext(auth.WithContext(req.Context(), tc.auth))
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			assertNilOrgForbidden(t, w, store.called)
		})
	}
}
