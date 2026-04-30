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
	lastOrgID  *uuid.UUID
	lastUserID *uuid.UUID
}

func (m *mockAnalyticsStore) GetAnalytics(_ context.Context, orgID *uuid.UUID, userID *uuid.UUID) (*AnalyticsData, error) {
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
					Content:     "frequently recalled memory",
					AccessCount: 42,
					ProjectID:   &pid,
					CreatedAt:   now,
				},
			},
			LeastRecalled: []MemoryRankItem{
				{
					ID:          uuid.New(),
					Content:     "rarely recalled memory",
					AccessCount: 1,
					CreatedAt:   now,
				},
			},
			DeadWeight: []MemoryRankItem{
				{
					ID:          uuid.New(),
					Content:     "never accessed memory",
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

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/analytics", nil)
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
	if resp.MostRecalled[0].Content != "frequently recalled memory" {
		t.Errorf("expected content 'frequently recalled memory', got %q", resp.MostRecalled[0].Content)
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

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/analytics", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/analytics", nil)
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

// TestAdminAnalytics_RoleTiers exercises the same three-tier scope rule used
// by the usage handler, but applied to memory analytics.
func TestAdminAnalytics_RoleTiers(t *testing.T) {
	adminOrg := uuid.New()
	otherOrg := uuid.New()
	adminUser := uuid.New()
	ownerOrg := uuid.New()
	ownerUser := uuid.New()
	memberOrg := uuid.New()
	memberUser := uuid.New()
	drillUser := uuid.New()

	cases := []struct {
		name           string
		auth           *auth.AuthContext
		urlOrgID       string
		query          string
		wantOrgID      *uuid.UUID
		wantUserID     *uuid.UUID
		wantOrgIDIsNil bool
	}{
		{
			name:           "admin no filter is global",
			auth:           &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator},
			wantOrgIDIsNil: true,
		},
		{
			name:      "admin org query drills in",
			auth:      &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator},
			query:     "org=" + otherOrg.String(),
			wantOrgID: &otherOrg,
		},
		{
			name:       "admin org and user drill",
			auth:       &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator},
			query:      "org=" + otherOrg.String() + "&user=" + drillUser.String(),
			wantOrgID:  &otherOrg,
			wantUserID: &drillUser,
		},
		{
			name:      "admin URL path overrides query",
			auth:      &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator},
			urlOrgID:  otherOrg.String(),
			query:     "org=" + adminOrg.String(),
			wantOrgID: &otherOrg,
		},
		{
			name:      "org_owner widening attempt blocked",
			auth:      &auth.AuthContext{UserID: ownerUser, OrgID: ownerOrg, Role: auth.RoleOrgOwner},
			query:     "org=" + otherOrg.String(),
			wantOrgID: &ownerOrg,
		},
		{
			name:       "org_owner user drill",
			auth:       &auth.AuthContext{UserID: ownerUser, OrgID: ownerOrg, Role: auth.RoleOrgOwner},
			query:      "user=" + drillUser.String(),
			wantOrgID:  &ownerOrg,
			wantUserID: &drillUser,
		},
		{
			name:       "member pinned to self ignores widening",
			auth:       &auth.AuthContext{UserID: memberUser, OrgID: memberOrg, Role: auth.RoleMember},
			query:      "org=" + otherOrg.String() + "&user=" + drillUser.String(),
			wantOrgID:  &memberOrg,
			wantUserID: &memberUser,
		},
		{
			name:       "readonly pinned to self",
			auth:       &auth.AuthContext{UserID: memberUser, OrgID: memberOrg, Role: auth.RoleReadonly},
			wantOrgID:  &memberOrg,
			wantUserID: &memberUser,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockAnalyticsStore{data: &AnalyticsData{}}
			h := NewAdminAnalyticsHandler(AnalyticsConfig{Store: store})

			url := "/v1/admin/analytics"
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

			if tc.wantOrgIDIsNil {
				if store.lastOrgID != nil {
					t.Errorf("expected OrgID nil (global), got %v", store.lastOrgID)
				}
			} else if tc.wantOrgID != nil {
				if store.lastOrgID == nil || *store.lastOrgID != *tc.wantOrgID {
					t.Errorf("expected OrgID %v, got %v", tc.wantOrgID, store.lastOrgID)
				}
			}

			if tc.wantUserID == nil {
				if store.lastUserID != nil {
					t.Errorf("expected UserID nil, got %v", store.lastUserID)
				}
			} else {
				if store.lastUserID == nil || *store.lastUserID != *tc.wantUserID {
					t.Errorf("expected UserID %v, got %v", tc.wantUserID, store.lastUserID)
				}
			}
		})
	}
}
