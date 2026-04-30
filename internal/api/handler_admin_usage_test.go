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

// --- mock UsageStore ---

type mockUsageStore struct {
	report    *UsageReport
	err       error
	lastFilter UsageFilter
}

func (m *mockUsageStore) QueryUsage(_ context.Context, filter UsageFilter) (*UsageReport, error) {
	m.lastFilter = filter
	return m.report, m.err
}

// --- helpers ---

func defaultUsageReport() *UsageReport {
	return &UsageReport{
		Groups: []UsageGroup{
			{Key: "enrich", TokensInput: 1000, TokensOutput: 500, CallCount: 10},
			{Key: "search", TokensInput: 200, TokensOutput: 100, CallCount: 5},
		},
		Totals: UsageTotals{
			TokensInput:  1200,
			TokensOutput: 600,
			CallCount:    15,
		},
	}
}

// --- tests ---

func TestAdminUsageDefaultParams(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp UsageReport
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Groups) != 2 {
		t.Errorf("expected 2 groups, got %d", len(resp.Groups))
	}
	if resp.Totals.TokensInput != 1200 {
		t.Errorf("expected tokens_input 1200, got %d", resp.Totals.TokensInput)
	}
	if resp.Totals.TokensOutput != 600 {
		t.Errorf("expected tokens_output 600, got %d", resp.Totals.TokensOutput)
	}
	if resp.Totals.CallCount != 15 {
		t.Errorf("expected call_count 15, got %d", resp.Totals.CallCount)
	}

	// Default group_by should be "operation".
	if store.lastFilter.GroupBy != "operation" {
		t.Errorf("expected group_by operation, got %q", store.lastFilter.GroupBy)
	}
	if store.lastFilter.OrgID != nil {
		t.Error("expected OrgID to be nil")
	}
	if store.lastFilter.UserID != nil {
		t.Error("expected UserID to be nil")
	}
	if store.lastFilter.ProjectID != nil {
		t.Error("expected ProjectID to be nil")
	}
	if store.lastFilter.From != nil {
		t.Error("expected From to be nil")
	}
	if store.lastFilter.To != nil {
		t.Error("expected To to be nil")
	}
}

func TestAdminUsageAllFilters(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	orgID := uuid.New()
	projectID := uuid.New()
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	url := "/v1/admin/usage?" +
		"project=" + projectID.String() +
		"&from=" + from.Format(time.RFC3339) +
		"&to=" + to.Format(time.RFC3339) +
		"&group_by=user"

	req := httptest.NewRequest(http.MethodGet, url, nil)
	// Administrator with no ?org= or URL path — global scope (OrgID nil).
	ac := &auth.AuthContext{UserID: uuid.New(), OrgID: orgID, Role: auth.RoleAdministrator}
	req = req.WithContext(auth.WithContext(req.Context(), ac))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.lastFilter.OrgID != nil {
		t.Errorf("expected nil OrgID for admin without ?org=, got %v", store.lastFilter.OrgID)
	}
	if store.lastFilter.UserID != nil {
		t.Errorf("expected nil UserID for admin, got %v", store.lastFilter.UserID)
	}
	if store.lastFilter.ProjectID == nil || *store.lastFilter.ProjectID != projectID {
		t.Errorf("expected ProjectID %s, got %v", projectID, store.lastFilter.ProjectID)
	}
	if store.lastFilter.From == nil || !store.lastFilter.From.Equal(from) {
		t.Errorf("expected From %v, got %v", from, store.lastFilter.From)
	}
	if store.lastFilter.To == nil || !store.lastFilter.To.Equal(to) {
		t.Errorf("expected To %v, got %v", to, store.lastFilter.To)
	}
	if store.lastFilter.GroupBy != "user" {
		t.Errorf("expected group_by user, got %q", store.lastFilter.GroupBy)
	}
}

func TestAdminUsageGroupByModel(t *testing.T) {
	store := &mockUsageStore{
		report: &UsageReport{
			Groups: []UsageGroup{
				{Key: "gpt-4.1-nano", TokensInput: 800, TokensOutput: 400, CallCount: 8},
			},
			Totals: UsageTotals{TokensInput: 800, TokensOutput: 400, CallCount: 8},
		},
	}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage?group_by=model", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp UsageReport
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(resp.Groups))
	}
	if resp.Groups[0].Key != "gpt-4.1-nano" {
		t.Errorf("expected group key gpt-4.1-nano, got %q", resp.Groups[0].Key)
	}
	if store.lastFilter.GroupBy != "model" {
		t.Errorf("expected group_by model, got %q", store.lastFilter.GroupBy)
	}
}

func TestAdminUsageInvalidGroupBy(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage?group_by=invalid", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error envelope")
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminUsageStoreError(t *testing.T) {
	store := &mockUsageStore{err: errors.New("database timeout")}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage", nil)
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

func TestAdminUsageWrongMethod(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/usage", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error envelope")
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminUsageFromToDates(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	from := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC)

	url := "/v1/admin/usage?from=" + from.Format(time.RFC3339) + "&to=" + to.Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.lastFilter.From == nil || !store.lastFilter.From.Equal(from) {
		t.Errorf("expected From %v, got %v", from, store.lastFilter.From)
	}
	if store.lastFilter.To == nil || !store.lastFilter.To.Equal(to) {
		t.Errorf("expected To %v, got %v", to, store.lastFilter.To)
	}
	// No UUID filters should be set.
	if store.lastFilter.OrgID != nil {
		t.Error("expected OrgID to be nil")
	}
	if store.lastFilter.UserID != nil {
		t.Error("expected UserID to be nil")
	}
	if store.lastFilter.ProjectID != nil {
		t.Error("expected ProjectID to be nil")
	}
	// Default group_by.
	if store.lastFilter.GroupBy != "operation" {
		t.Errorf("expected group_by operation, got %q", store.lastFilter.GroupBy)
	}
}

// TestAdminUsageRoleTiers exercises the three-tier scope rule:
//
//   - administrator: global by default; URL path > ?org= drills in; ?user= drills further.
//   - org_owner: pinned to own org (cannot widen); ?user= optionally drills into one user.
//   - member/readonly/service: pinned to own org and own user (widening attempts ignored).
func TestAdminUsageRoleTiers(t *testing.T) {
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
		urlOrgID       string // injected as chi URL param when non-empty
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
		{
			name:       "service pinned to self",
			auth:       &auth.AuthContext{UserID: memberUser, OrgID: memberOrg, Role: auth.RoleService},
			wantOrgID:  &memberOrg,
			wantUserID: &memberUser,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockUsageStore{report: defaultUsageReport()}
			h := NewAdminUsageHandler(UsageConfig{Store: store})

			url := "/v1/admin/usage"
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
				if store.lastFilter.OrgID != nil {
					t.Errorf("expected OrgID nil (global), got %v", store.lastFilter.OrgID)
				}
			} else if tc.wantOrgID != nil {
				if store.lastFilter.OrgID == nil || *store.lastFilter.OrgID != *tc.wantOrgID {
					t.Errorf("expected OrgID %v, got %v", tc.wantOrgID, store.lastFilter.OrgID)
				}
			}

			if tc.wantUserID == nil {
				if store.lastFilter.UserID != nil {
					t.Errorf("expected UserID nil, got %v", store.lastFilter.UserID)
				}
			} else {
				if store.lastFilter.UserID == nil || *store.lastFilter.UserID != *tc.wantUserID {
					t.Errorf("expected UserID %v, got %v", tc.wantUserID, store.lastFilter.UserID)
				}
			}
		})
	}
}

func TestAdminUsageInvalidUUIDIgnored(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage?org=not-a-uuid", nil)
	// Admin context — invalid UUID should be silently ignored.
	ac := &auth.AuthContext{UserID: uuid.New(), Role: auth.RoleAdministrator}
	req = req.WithContext(auth.WithContext(req.Context(), ac))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Invalid UUID should be silently ignored — OrgID remains nil.
	if store.lastFilter.OrgID != nil {
		t.Errorf("expected OrgID to be nil for invalid UUID, got %v", store.lastFilter.OrgID)
	}
}
