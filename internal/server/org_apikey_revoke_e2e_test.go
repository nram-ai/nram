package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
	adminstore "github.com/nram-ai/nram/internal/storage/admin"
)

// TestOrgRevokeAPIKey_CrossTenantIsolation exercises the real org users handler
// (NewOrgUsersHandler backed by the real UserAdminStore and repos) to confirm
// that DELETE /v1/orgs/{org_id}/users/{id}/api-keys/{key_id} cannot revoke an
// API key that belongs to a user in a different organization. The org handler
// verifies the path user is in the caller's org, and the scoped delete refuses
// to touch a key whose owner differs from that user.
func TestOrgRevokeAPIKey_CrossTenantIsolation(t *testing.T) {
	db := e2eTestDB(t)
	ctx := context.Background()

	nsRepo := storage.NewNamespaceRepo(db)
	orgRepo := storage.NewOrganizationRepo(db)
	userRepo := storage.NewUserRepo(db)
	apiKeyRepo := storage.NewAPIKeyRepo(db)
	projectRepo := storage.NewProjectRepo(db)

	rootID := uuid.Nil

	// makeOrg creates an org + its namespace and returns the org together with
	// the namespace path, so callers never reconstruct the path format.
	makeOrg := func(name, slug string) (*model.Organization, string) {
		t.Helper()
		nsID := uuid.New()
		nsPath := "root/" + slug + "-ns"
		ns := &model.Namespace{
			ID:       nsID,
			Name:     name + " NS",
			Slug:     slug + "-ns",
			Kind:     "org",
			ParentID: &rootID,
			Path:     nsPath,
			Depth:    1,
		}
		if err := nsRepo.Create(ctx, ns); err != nil {
			t.Fatalf("create %s namespace: %v", name, err)
		}
		org := &model.Organization{NamespaceID: nsID, Name: name, Slug: slug}
		if err := orgRepo.Create(ctx, org); err != nil {
			t.Fatalf("create %s org: %v", name, err)
		}
		return org, nsPath
	}

	makeUserWithKey := func(email string, orgID uuid.UUID, orgNSPath string) (*model.User, uuid.UUID) {
		t.Helper()
		user := &model.User{Email: email, DisplayName: email, OrgID: orgID, Role: "member"}
		if err := userRepo.Create(ctx, user, nsRepo, nil, orgNSPath); err != nil {
			t.Fatalf("create user %s: %v", email, err)
		}
		key := &model.APIKey{UserID: user.ID, Name: email + " key", Scopes: []uuid.UUID{}}
		if _, err := apiKeyRepo.Create(ctx, key); err != nil {
			t.Fatalf("create key for %s: %v", email, err)
		}
		return user, key.ID
	}

	orgA, orgANSPath := makeOrg("Acme", "acme")
	orgB, orgBNSPath := makeOrg("Globex", "globex")
	userA, keyA := makeUserWithKey("a@acme.test", orgA.ID, orgANSPath)
	_, keyB := makeUserWithKey("b@globex.test", orgB.ID, orgBNSPath)

	store := adminstore.NewUserAdminStore(userRepo, apiKeyRepo, nsRepo, orgRepo, projectRepo)
	handler := api.NewOrgUsersHandler(api.OrgUserConfig{Store: store})

	router := chi.NewRouter()
	router.Handle("/v1/orgs/{org_id}/users/*", handler)

	revoke := func(orgID, userID, keyID uuid.UUID) int {
		url := fmt.Sprintf("/v1/orgs/%s/users/%s/api-keys/%s", orgID, userID, keyID)
		req := httptest.NewRequest(http.MethodDelete, url, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code
	}

	// Cross-tenant: org A owner passes a real user in org A but a key owned by a
	// user in org B. The key must NOT be deleted and the response must be 404.
	if code := revoke(orgA.ID, userA.ID, keyB); code != http.StatusNotFound {
		t.Fatalf("cross-tenant revoke: expected 404, got %d", code)
	}
	if _, err := apiKeyRepo.GetByID(ctx, keyB); err != nil {
		t.Fatalf("cross-tenant key was deleted or unreadable: %v", err)
	}

	// Same-org: revoking a key owned by the path user in the caller's org works.
	if code := revoke(orgA.ID, userA.ID, keyA); code != http.StatusNoContent {
		t.Fatalf("same-org revoke: expected 204, got %d", code)
	}
	if _, err := apiKeyRepo.GetByID(ctx, keyA); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("same-org key should be gone, got err=%v", err)
	}
}
