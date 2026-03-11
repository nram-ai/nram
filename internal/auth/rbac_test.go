package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// rbacOKHandler is a simple handler that writes 200 OK.
var rbacOKHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})

// makeRequest creates a request whose context already contains an AuthContext
// with the specified role. If role is empty, no AuthContext is set.
func makeRequest(role string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if role != "" {
		ac := &AuthContext{
			UserID: uuid.New(),
			Role:   role,
		}
		r = r.WithContext(WithContext(r.Context(), ac))
	}
	return r
}

func TestRBACRoleLevel(t *testing.T) {
	tests := []struct {
		role  string
		level int
	}{
		{RoleAdministrator, 40},
		{RoleOrgOwner, 30},
		{RoleMember, 20},
		{RoleReadonly, 10},
		{RoleService, 20},
		{"unknown", 0},
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			got := RoleLevel(tt.role)
			if got != tt.level {
				t.Errorf("RoleLevel(%q) = %d, want %d", tt.role, got, tt.level)
			}
		})
	}
}

func TestRBACHierarchyOrder(t *testing.T) {
	if RoleLevel(RoleAdministrator) <= RoleLevel(RoleOrgOwner) {
		t.Error("administrator should outrank org_owner")
	}
	if RoleLevel(RoleOrgOwner) <= RoleLevel(RoleMember) {
		t.Error("org_owner should outrank member")
	}
	if RoleLevel(RoleMember) <= RoleLevel(RoleReadonly) {
		t.Error("member should outrank readonly")
	}
	if RoleLevel(RoleReadonly) <= 0 {
		t.Error("readonly should be above zero")
	}
}

func TestRBACHasPermission(t *testing.T) {
	tests := []struct {
		user     string
		required string
		allowed  bool
	}{
		{RoleAdministrator, RoleAdministrator, true},
		{RoleAdministrator, RoleMember, true},
		{RoleMember, RoleMember, true},
		{RoleMember, RoleAdministrator, false},
		{RoleReadonly, RoleMember, false},
		{RoleReadonly, RoleReadonly, true},
		{RoleService, RoleMember, true},
		{RoleService, RoleOrgOwner, false},
		{RoleOrgOwner, RoleMember, true},
	}

	for _, tt := range tests {
		t.Run(tt.user+"_needs_"+tt.required, func(t *testing.T) {
			got := HasPermission(tt.user, tt.required)
			if got != tt.allowed {
				t.Errorf("HasPermission(%q, %q) = %v, want %v", tt.user, tt.required, got, tt.allowed)
			}
		})
	}
}

func TestRBACRequireRoleAdminAccess(t *testing.T) {
	mw := RequireRole(RoleAdministrator)
	handler := mw(rbacOKHandler)

	// Administrator can access admin-only routes.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleAdministrator))
	if rec.Code != http.StatusOK {
		t.Errorf("admin accessing admin route: got %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestRBACRequireRoleMemberDeniedAdmin(t *testing.T) {
	mw := RequireRole(RoleAdministrator)
	handler := mw(rbacOKHandler)

	// Member cannot access admin-only routes.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleMember))
	if rec.Code != http.StatusForbidden {
		t.Errorf("member accessing admin route: got %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestRBACRequireRoleReadonlyDeniedMember(t *testing.T) {
	mw := RequireRole(RoleMember)
	handler := mw(rbacOKHandler)

	// Readonly cannot access member-only routes.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleReadonly))
	if rec.Code != http.StatusForbidden {
		t.Errorf("readonly accessing member route: got %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestRBACRequireRoleNoAuthContext(t *testing.T) {
	mw := RequireRole(RoleMember)
	handler := mw(rbacOKHandler)

	// No AuthContext should return 401.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(""))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no auth context: got %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRBACRequireAnyRole(t *testing.T) {
	mw := RequireAnyRole(RoleAdministrator, RoleService)
	handler := mw(rbacOKHandler)

	// Service role is explicitly listed, should pass.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleService))
	if rec.Code != http.StatusOK {
		t.Errorf("service with RequireAnyRole: got %d, want %d", rec.Code, http.StatusOK)
	}

	// Administrator is explicitly listed, should pass.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleAdministrator))
	if rec.Code != http.StatusOK {
		t.Errorf("admin with RequireAnyRole: got %d, want %d", rec.Code, http.StatusOK)
	}

	// Member is not listed, should fail.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleMember))
	if rec.Code != http.StatusForbidden {
		t.Errorf("member with RequireAnyRole: got %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestRBACRequireAnyRoleNoAuthContext(t *testing.T) {
	mw := RequireAnyRole(RoleMember)
	handler := mw(rbacOKHandler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(""))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no auth context with RequireAnyRole: got %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRBACServiceRoleHandling(t *testing.T) {
	// Service has the same numeric level as member, so it can access
	// member-level routes through the hierarchy.
	mwMember := RequireRole(RoleMember)
	handler := mwMember(rbacOKHandler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleService))
	if rec.Code != http.StatusOK {
		t.Errorf("service accessing member route: got %d, want %d", rec.Code, http.StatusOK)
	}

	// Service cannot access org_owner routes through hierarchy alone.
	mwOwner := RequireRole(RoleOrgOwner)
	handler = mwOwner(rbacOKHandler)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleService))
	if rec.Code != http.StatusForbidden {
		t.Errorf("service accessing org_owner route: got %d, want %d", rec.Code, http.StatusForbidden)
	}

	// But service can access org_owner routes if explicitly allowed via RequireAnyRole.
	mwAny := RequireAnyRole(RoleOrgOwner, RoleService)
	handler = mwAny(rbacOKHandler)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, makeRequest(RoleService))
	if rec.Code != http.StatusOK {
		t.Errorf("service with RequireAnyRole(org_owner, service): got %d, want %d", rec.Code, http.StatusOK)
	}
}
