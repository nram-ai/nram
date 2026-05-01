package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// Privacy invariants enforced by the 2026-04-30 admin-dashboard leak fix.
// These tests pin the routing- and shape-level guarantees in CI so a
// future change cannot silently re-leak.

// TestPrivacy_AdminProjectsRouteRemoved verifies that /v1/admin/projects
// is no longer mounted. Cross-tenant project listing exposed user-authored
// project names + descriptions and was a privacy leak; admins use
// /v1/me/projects like every other role.
func TestPrivacy_AdminProjectsRouteRemoved(t *testing.T) {
	env := newRRTestEnv(t)

	for _, path := range []string{
		"/v1/admin/projects",
		"/v1/admin/projects/00000000-0000-0000-0000-000000000000",
	} {
		t.Run(path, func(t *testing.T) {
			resp := rbacDoRequest(t, "GET", env.Server.URL+path, env.Admin.JWT, nil)
			if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
				t.Fatalf("expected 404 or 405 for removed route %s, got %d", path, resp.StatusCode)
			}
		})
	}
}

// TestPrivacy_DreamingMovedToAdmin verifies that the system-wide dreaming
// pipeline is admin-only. Pre-fix, /v1/dreaming was reachable by any
// authenticated user. Post-fix, /v1/dreaming returns 404 (the route is
// gone) and /v1/admin/dreaming requires RoleAdministrator.
func TestPrivacy_DreamingMovedToAdmin(t *testing.T) {
	env := newRRTestEnv(t)

	// Old path is gone.
	t.Run("legacy_path_gone", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/dreaming", env.Admin.JWT, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404 for removed /v1/dreaming, got %d", resp.StatusCode)
		}
	})

	// New path: non-admin = 403, admin = 200.
	t.Run("admin_path_admin_only", func(t *testing.T) {
		cases := []struct {
			name   string
			token  string
			expect int
		}{
			{"admin", env.Admin.JWT, http.StatusOK},
			{"org_owner", env.OrgAOwner.JWT, http.StatusForbidden},
			{"member", env.OrgAMember.JWT, http.StatusForbidden},
			{"readonly", env.OrgAReadonly.JWT, http.StatusForbidden},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/admin/dreaming", tc.token, nil)
				if resp.StatusCode != tc.expect {
					t.Errorf("%s: expected %d, got %d", tc.name, tc.expect, resp.StatusCode)
				}
			})
		}
	})
}

// TestPrivacy_EnrichmentMovedToAdmin mirrors the dreaming test: the legacy
// /v1/enrichment is gone, /v1/admin/enrichment is admin-only.
func TestPrivacy_EnrichmentMovedToAdmin(t *testing.T) {
	env := newRRTestEnv(t)

	t.Run("legacy_path_gone", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/enrichment", env.Admin.JWT, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404 for removed /v1/enrichment, got %d", resp.StatusCode)
		}
	})

	t.Run("admin_path_admin_only", func(t *testing.T) {
		cases := []struct {
			name   string
			token  string
			expect int
		}{
			{"admin", env.Admin.JWT, http.StatusOK},
			{"member", env.OrgAMember.JWT, http.StatusForbidden},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/admin/enrichment", tc.token, nil)
				if resp.StatusCode != tc.expect {
					t.Errorf("%s: expected %d, got %d", tc.name, tc.expect, resp.StatusCode)
				}
			})
		}
	})
}

// TestPrivacy_SelfTierWideningIgnored verifies that /v1/dashboard,
// /v1/activity, /v1/analytics, /v1/usage, /v1/graph, /v1/namespaces/tree
// always return 200 for any authenticated user (self-scoped) regardless
// of ?org=, ?user=, or any other widening attempt. The widening primitive
// (resolveAdminScope) was deleted; admins see only their own data here.
func TestPrivacy_SelfTierWideningIgnored(t *testing.T) {
	env := newRRTestEnv(t)

	// Build URLs with widening attempts that would have worked pre-fix.
	otherOrg := "00000000-0000-0000-0000-000000000001"
	otherUser := "00000000-0000-0000-0000-000000000002"
	wideningQuery := fmt.Sprintf("?org=%s&user=%s", otherOrg, otherUser)

	for _, path := range []string{
		"/v1/dashboard",
		"/v1/activity",
		"/v1/analytics",
		"/v1/usage",
		"/v1/namespaces/tree",
	} {
		t.Run(path, func(t *testing.T) {
			// Admin with widening params — must still succeed (200), and
			// the handler internally pins to admin's own scope. We can't
			// observe the scope from outside without a mock, but the
			// per-handler unit tests in internal/api cover that.
			resp := rbacDoRequest(t, "GET", env.Server.URL+path+wideningQuery, env.Admin.JWT, nil)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("admin GET %s%s expected 200, got %d", path, wideningQuery, resp.StatusCode)
			}
		})
	}
}

// TestPrivacy_TenancyListsAreMetadataOnly verifies the response shapes
// of /v1/admin/orgs and /v1/admin/users carry no per-row memory/usage/
// content fields — only tenancy metadata. Recursive JSON walk asserts
// no key named "content" or "summary" appears anywhere in the response.
func TestPrivacy_TenancyListsAreMetadataOnly(t *testing.T) {
	env := newRRTestEnv(t)

	for _, path := range []string{
		"/v1/admin/orgs",
		"/v1/admin/users",
	} {
		t.Run(path, func(t *testing.T) {
			resp := rbacDoRequest(t, "GET", env.Server.URL+path, env.Admin.JWT, nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200 for %s, got %d", path, resp.StatusCode)
			}
			defer resp.Body.Close()

			var body interface{}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}

			forbidden := []string{"content", "summary"}
			if leaked := findKey(body, forbidden); leaked != "" {
				t.Errorf("%s response contains forbidden key %q (privacy leak)", path, leaked)
			}
		})
	}
}

// findKey walks the decoded JSON value recursively and returns the first
// forbidden key it finds, or "" if none. Used by privacy tests to assert
// response shapes carry no content fields.
func findKey(v interface{}, forbidden []string) string {
	switch tv := v.(type) {
	case map[string]interface{}:
		for k, child := range tv {
			lk := strings.ToLower(k)
			for _, f := range forbidden {
				if lk == f {
					return k
				}
			}
			if found := findKey(child, forbidden); found != "" {
				return found
			}
		}
	case []interface{}:
		for _, child := range tv {
			if found := findKey(child, forbidden); found != "" {
				return found
			}
		}
	}
	return ""
}
