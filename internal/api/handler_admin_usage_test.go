package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// --- mock UsageStore ---

type mockUsageStore struct {
	report     *UsageReport
	err        error
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
			{Key: "enrich", TokensInput: 1000, TokensOutput: 500,
				TokensCacheRead: 800, TokensCacheWrite: 25, CallCount: 10},
			{Key: "search", TokensInput: 200, TokensOutput: 100, CallCount: 5},
		},
		Totals: UsageTotals{
			TokensInput:      1200,
			TokensOutput:     600,
			TokensCacheRead:  800,
			TokensCacheWrite: 25,
			CallCount:        15,
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

	// Capture the raw body before decoding: json.Decoder drains the buffer, so
	// a later w.Body.String() would read empty.
	body := w.Body.String()

	var resp UsageReport
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
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
	// The cache buckets must survive JSON round-trip under their snake_case
	// tags; the SPA and the OpenAPI schema both key off these names.
	if resp.Totals.TokensCacheRead != 800 {
		t.Errorf("expected tokens_cache_read 800, got %d", resp.Totals.TokensCacheRead)
	}
	if resp.Totals.TokensCacheWrite != 25 {
		t.Errorf("expected tokens_cache_write 25, got %d", resp.Totals.TokensCacheWrite)
	}
	if resp.Groups[0].TokensCacheRead != 800 {
		t.Errorf("expected group tokens_cache_read 800, got %d", resp.Groups[0].TokensCacheRead)
	}
	for _, key := range []string{`"tokens_cache_read"`, `"tokens_cache_write"`} {
		if !strings.Contains(body, key) {
			t.Errorf("response missing JSON key %s: %s", key, body)
		}
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
	adminUser := uuid.New()
	ac := &auth.AuthContext{UserID: adminUser, OrgID: orgID, Role: auth.RoleAdministrator}
	req = req.WithContext(auth.WithContext(req.Context(), ac))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Self-tier post-fix: admin sees their own org+user, not global.
	if store.lastFilter.OrgID == nil || *store.lastFilter.OrgID != orgID {
		t.Errorf("expected OrgID = admin's own org %v, got %v", orgID, store.lastFilter.OrgID)
	}
	if store.lastFilter.UserID == nil || *store.lastFilter.UserID != adminUser {
		t.Errorf("expected UserID = admin's own user %v, got %v", adminUser, store.lastFilter.UserID)
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

// TestAdminUsageSelfScope verifies the post-fix tier-A semantics: /v1/usage
// always pins to the caller's own scope. Widening attempts via ?org=, ?user=,
// or URL-path org_id are ignored regardless of role. The widening capability
// was removed in the 2026-04-30 leak fix; admin's cross-tenant view moves to
// /v1/admin/system/usage and /v1/orgs/{id}/usage.
func TestAdminUsageSelfScope(t *testing.T) {
	adminOrg := uuid.New()
	otherOrg := uuid.New()
	adminUser := uuid.New()
	otherUser := uuid.New()

	cases := []struct {
		name     string
		auth     *auth.AuthContext
		query    string
		urlOrgID string
	}{
		{name: "admin alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}},
		{name: "admin ?org=other ignored", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}, query: "org=" + otherOrg.String()},
		{name: "admin ?user=other ignored", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}, query: "user=" + otherUser.String()},
		{name: "admin URL path ignored", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleAdministrator}, urlOrgID: otherOrg.String()},
		{name: "org_owner alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleOrgOwner}},
		{name: "member alone", auth: &auth.AuthContext{UserID: adminUser, OrgID: adminOrg, Role: auth.RoleMember}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockUsageStore{report: defaultUsageReport()}
			h := NewAdminUsageHandler(UsageConfig{Store: store})

			url := "/v1/usage"
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

			// Self-tier: caller's own org+user, no exceptions.
			if store.lastFilter.OrgID == nil || *store.lastFilter.OrgID != tc.auth.OrgID {
				t.Errorf("expected OrgID = caller's org %v, got %v", tc.auth.OrgID, store.lastFilter.OrgID)
			}
			if store.lastFilter.UserID == nil || *store.lastFilter.UserID != tc.auth.UserID {
				t.Errorf("expected UserID = caller's user %v, got %v", tc.auth.UserID, store.lastFilter.UserID)
			}
		})
	}
}

// TestAdminUsageQueryParamsIgnoredOnSelfTier verifies that the post-fix
// self-tier handler ignores ?org= entirely (regardless of well-formedness).
// The widening param is gone; only the auth context determines scope.
func TestAdminUsageQueryParamsIgnoredOnSelfTier(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	someOrg := uuid.New()
	for _, q := range []string{"org=not-a-uuid", "org=" + uuid.New().String(), ""} {
		req := httptest.NewRequest(http.MethodGet, "/v1/usage?"+q, nil)
		adminUser := uuid.New()
		ac := &auth.AuthContext{UserID: adminUser, OrgID: someOrg, Role: auth.RoleAdministrator}
		req = req.WithContext(auth.WithContext(req.Context(), ac))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}

		// Always pinned to caller's own org.
		if store.lastFilter.OrgID == nil || *store.lastFilter.OrgID != someOrg {
			t.Errorf("query %q: expected OrgID = caller's org, got %v", q, store.lastFilter.OrgID)
		}
	}
}
