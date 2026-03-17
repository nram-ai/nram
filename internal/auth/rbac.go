package auth

import (
	"net/http"
)

// Role constants define the five roles supported by the RBAC system.
const (
	RoleAdministrator = "administrator"
	RoleOrgOwner      = "org_owner"
	RoleMember        = "member"
	RoleReadonly       = "readonly"
	RoleService       = "service"
)

// roleLevels maps each role to a numeric level for hierarchy comparison.
// Higher values indicate more privileges. The service role is assigned
// a level equivalent to member; it is intended for machine-to-machine
// access and must be explicitly allowed via RequireAnyRole when access
// beyond the member level is needed.
var roleLevels = map[string]int{
	RoleAdministrator: 40,
	RoleOrgOwner:      30,
	RoleMember:        20,
	RoleReadonly:       10,
	RoleService:       20, // same privilege tier as member by default
}

// RoleLevel returns the numeric privilege level for the given role.
// Unknown roles return 0.
func RoleLevel(role string) int {
	return roleLevels[role]
}

// HasPermission reports whether userRole meets or exceeds the privilege
// level of requiredRole.
func HasPermission(userRole, requiredRole string) bool {
	return RoleLevel(userRole) >= RoleLevel(requiredRole)
}

// RequireRole returns middleware that enforces a minimum role level.
// If the request has no AuthContext, it responds with 401 Unauthorized.
// If the authenticated user's role does not meet the minimum, it responds
// with 403 Forbidden.
func RequireRole(minRole string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ac := FromContext(r.Context())
			if ac == nil {
				http.Error(w, "authentication required", http.StatusUnauthorized)
				return
			}

			if !HasPermission(ac.Role, minRole) {
				http.Error(w, "forbidden: insufficient role", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RequireWriteAccess returns middleware that blocks requests from users with
// the readonly role on mutating HTTP methods (POST, PUT, PATCH, DELETE).
// GET and HEAD requests are always allowed through.
func RequireWriteAccess() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet || r.Method == http.MethodHead {
				next.ServeHTTP(w, r)
				return
			}

			ac := FromContext(r.Context())
			if ac == nil {
				http.Error(w, "authentication required", http.StatusUnauthorized)
				return
			}

			if ac.Role == RoleReadonly {
				http.Error(w, "forbidden: readonly users cannot perform write operations", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RequireAnyRole returns middleware that checks whether the authenticated
// user holds any one of the listed roles. This is useful for granting
// access to the service role alongside specific human roles without
// relying on the hierarchy.
func RequireAnyRole(roles ...string) func(http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(roles))
	for _, r := range roles {
		allowed[r] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ac := FromContext(r.Context())
			if ac == nil {
				http.Error(w, "authentication required", http.StatusUnauthorized)
				return
			}

			if _, ok := allowed[ac.Role]; !ok {
				http.Error(w, "forbidden: insufficient role", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
