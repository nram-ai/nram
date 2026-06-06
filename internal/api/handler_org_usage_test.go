package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// orgUsageRequest builds a chi-routed request that has {org_id} in the URL
// params, since OrgScope reads via chi.URLParam.
func orgUsageRequest(t *testing.T, h http.HandlerFunc, orgID uuid.UUID, ac *auth.AuthContext) *httptest.ResponseRecorder {
	t.Helper()
	r := chi.NewRouter()
	r.Get("/v1/orgs/{org_id}/usage", h)

	req := httptest.NewRequest(http.MethodGet, "/v1/orgs/"+orgID.String()+"/usage", nil)
	if ac != nil {
		req = req.WithContext(auth.WithContext(req.Context(), ac))
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestOrgUsage_ScopesToOrgNotUser is the regression test for the org-tier
// token-usage bug: before the per-tier handler split, /v1/orgs/{id}/usage
// reused the self-tier handler and silently filtered to the calling user's
// own tokens. The handler must scope by OrgID (org-wide) and leave UserID
// nil so every user in the org rolls up.
func TestOrgUsage_ScopesToOrgNotUser(t *testing.T) {
	orgID := uuid.New()
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewOrgUsageHandler(UsageConfig{Store: store})

	ac := &auth.AuthContext{
		UserID: uuid.New(),
		OrgID:  orgID,
		Role:   auth.RoleOrgOwner,
	}
	w := orgUsageRequest(t, h, orgID, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if store.lastFilter.OrgID == nil || *store.lastFilter.OrgID != orgID {
		t.Errorf("expected OrgID=%v, got %v", orgID, store.lastFilter.OrgID)
	}
	if store.lastFilter.UserID != nil {
		t.Errorf("org-tier must not scope by UserID (org-wide rollup), got %v", store.lastFilter.UserID)
	}
}

func TestOrgUsage_RejectsMember(t *testing.T) {
	orgID := uuid.New()
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewOrgUsageHandler(UsageConfig{Store: store})

	ac := &auth.AuthContext{
		UserID: uuid.New(),
		OrgID:  orgID,
		Role:   auth.RoleMember,
	}
	w := orgUsageRequest(t, h, orgID, ac)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-owner, got %d", w.Code)
	}
}

func TestOrgUsage_AdminCanQueryAnyOrg(t *testing.T) {
	orgID := uuid.New()
	otherOrg := uuid.New()
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewOrgUsageHandler(UsageConfig{Store: store})

	// Administrator reads someone else's org; requireOrgOwner short-circuits
	// admins through.
	ac := &auth.AuthContext{
		UserID: uuid.New(),
		OrgID:  otherOrg,
		Role:   auth.RoleAdministrator,
	}
	w := orgUsageRequest(t, h, orgID, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for admin cross-org, got %d: %s", w.Code, w.Body.String())
	}
	if store.lastFilter.OrgID == nil || *store.lastFilter.OrgID != orgID {
		t.Errorf("expected OrgID=%v (URL param, not caller's org), got %v", orgID, store.lastFilter.OrgID)
	}
}
