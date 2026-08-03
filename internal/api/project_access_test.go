package api

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

func TestPathWithinOrg(t *testing.T) {
	cases := []struct {
		nsPath, orgPath string
		want            bool
	}{
		{"acme", "acme", true},                // exact
		{"acme/user/proj", "acme", true},      // descendant
		{"acmecorp/user/proj", "acme", false}, // sibling prefix — the SEC-25 bug
		{"acme2", "acme", false},              // bare prefix
		{"other", "acme", false},              // unrelated
	}
	for _, c := range cases {
		if got := pathWithinOrg(c.nsPath, c.orgPath); got != c.want {
			t.Errorf("pathWithinOrg(%q, %q) = %v, want %v", c.nsPath, c.orgPath, got, c.want)
		}
	}
}

// TestCheckProjectOrgAccess_CrossOrgPrefixBoundary pins SEC-25 end to end: a
// caller whose org namespace path is a bare string prefix of another org's must
// be denied that org's project, while same-org access still passes. It reuses
// the fakeProjLookup/fakeNSLookup/fakeOrgLookup/fakeUserLookup helpers from
// handler_move_test.go (same package).
func TestCheckProjectOrgAccess_CrossOrgPrefixBoundary(t *testing.T) {
	callerOrgNS := uuid.New()
	callerOrg := uuid.New()
	callerUser := uuid.New()

	foreignProjNS := uuid.New()
	foreignProj := uuid.New()

	ownProjNS := uuid.New()
	ownProj := uuid.New()

	cfg := ProjectAccessConfig{
		Users: fakeUserLookup{users: map[uuid.UUID]*model.User{callerUser: {OrgID: callerOrg}}},
		Orgs:  fakeOrgLookup{orgs: map[uuid.UUID]*model.Organization{callerOrg: {NamespaceID: callerOrgNS}}},
		Namespaces: fakeNSLookup{namespaces: map[uuid.UUID]*model.Namespace{
			callerOrgNS:   {Path: "acme"},
			foreignProjNS: {Path: "acmecorp/" + uuid.NewString() + "/" + uuid.NewString()},
			ownProjNS:     {Path: "acme/" + uuid.NewString() + "/" + uuid.NewString()},
		}},
		Projects: fakeProjLookup{projects: map[uuid.UUID]*model.Project{
			foreignProj: {NamespaceID: foreignProjNS},
			ownProj:     {NamespaceID: ownProjNS},
		}},
	}
	ac := &auth.AuthContext{UserID: callerUser, OrgID: callerOrg, Role: auth.RoleMember}

	if err := CheckProjectOrgAccess(context.Background(), cfg, ac, foreignProj); err == nil {
		t.Fatal("caller in org 'acme' must be DENIED a project in org 'acmecorp' (prefix collision)")
	}
	if err := CheckProjectOrgAccess(context.Background(), cfg, ac, ownProj); err != nil {
		t.Fatalf("caller in org 'acme' must be ALLOWED its own project, got %v", err)
	}
}
