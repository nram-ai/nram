package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// newTestOAuthClient creates an OAuth client for testing, inserting it into the DB
// via the repo so FK references are satisfied.
func newTestOAuthClient(t *testing.T, ctx context.Context, repo *OAuthRepo, user *model.User) *model.OAuthClient {
	t.Helper()
	secret := "test-secret-" + uuid.New().String()[:8]
	client := &model.OAuthClient{
		ClientID:       "cid-" + uuid.New().String()[:8],
		ClientSecret:   &secret,
		Name:           "Test Client",
		RedirectURIs:   []string{"https://example.com/callback"},
		GrantTypes:     []string{"authorization_code", "refresh_token"},
		OrgID:          &user.OrgID,
		UserID:         &user.ID,
		AutoRegistered: false,
	}
	if err := repo.CreateClient(ctx, client); err != nil {
		t.Fatalf("failed to create test oauth client: %v", err)
	}
	return client
}

// ---------------------------------------------------------------------------
// OAuth Client tests
// ---------------------------------------------------------------------------

func TestOAuthRepo_CreateClient(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		suffix := uuid.New().String()[:8]
		secret := "s3cret-" + suffix
		clientID := "my-client-" + suffix
		client := &model.OAuthClient{
			ClientID:       clientID,
			ClientSecret:   &secret,
			Name:           "My App " + suffix,
			RedirectURIs:   []string{"https://app.example.com/cb"},
			GrantTypes:     []string{"authorization_code"},
			OrgID:          &user.OrgID,
			AutoRegistered: true,
		}
		if err := repo.CreateClient(ctx, client); err != nil {
			t.Fatalf("CreateClient failed: %v", err)
		}

		if client.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if client.ClientID != clientID {
			t.Fatalf("unexpected client_id: %q", client.ClientID)
		}
		if client.ClientSecret == nil || *client.ClientSecret != secret {
			t.Fatalf("unexpected client_secret: %v", client.ClientSecret)
		}
		if client.Name != "My App "+suffix {
			t.Fatalf("unexpected name: %q", client.Name)
		}
		if len(client.RedirectURIs) != 1 || client.RedirectURIs[0] != "https://app.example.com/cb" {
			t.Fatalf("unexpected redirect_uris: %v", client.RedirectURIs)
		}
		if len(client.GrantTypes) != 1 || client.GrantTypes[0] != "authorization_code" {
			t.Fatalf("unexpected grant_types: %v", client.GrantTypes)
		}
		if !client.AutoRegistered {
			t.Fatal("expected auto_registered to be true")
		}
		if client.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
	})
}

func TestOAuthRepo_CreateClient_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		suffix := uuid.New().String()[:8]
		client := &model.OAuthClient{
			ClientID:     "gen-id-client-" + suffix,
			Name:         "Gen " + suffix,
			RedirectURIs: []string{},
			GrantTypes:   []string{},
			OrgID:        &user.OrgID,
		}
		if err := repo.CreateClient(ctx, client); err != nil {
			t.Fatalf("CreateClient failed: %v", err)
		}
		if client.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestOAuthRepo_CreateClient_NilSlicesDefault(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		suffix := uuid.New().String()[:8]
		client := &model.OAuthClient{
			ClientID: "nil-slices-" + suffix,
			Name:     "Nil " + suffix,
			OrgID:    &user.OrgID,
		}
		if err := repo.CreateClient(ctx, client); err != nil {
			t.Fatalf("CreateClient failed: %v", err)
		}
		if client.RedirectURIs == nil {
			t.Fatal("expected non-nil redirect_uris")
		}
		if client.GrantTypes == nil {
			t.Fatal("expected non-nil grant_types")
		}
	})
}

func TestOAuthRepo_GetClientByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		client := newTestOAuthClient(t, ctx, repo, user)

		fetched, err := repo.GetClientByID(ctx, client.ClientID)
		if err != nil {
			t.Fatalf("GetClientByID failed: %v", err)
		}
		if fetched.ID != client.ID {
			t.Fatalf("expected ID %s, got %s", client.ID, fetched.ID)
		}
		if fetched.ClientID != client.ClientID {
			t.Fatalf("expected client_id %q, got %q", client.ClientID, fetched.ClientID)
		}
		if fetched.Name != client.Name {
			t.Fatalf("expected name %q, got %q", client.Name, fetched.Name)
		}
	})
}

func TestOAuthRepo_GetClientByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		_, err := repo.GetClientByID(ctx, "nonexistent-client-"+uuid.New().String()[:8])
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestOAuthRepo_ListClientsByUser(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		// Create two clients for the user's org
		newTestOAuthClient(t, ctx, repo, user)
		newTestOAuthClient(t, ctx, repo, user)

		results, err := repo.ListClientsByUser(ctx, user.ID)
		if err != nil {
			t.Fatalf("ListClientsByUser failed: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 clients, got %d", len(results))
		}
	})
}

func TestOAuthRepo_ListClientsByUser_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		results, err := repo.ListClientsByUser(ctx, user.ID)
		if err != nil {
			t.Fatalf("ListClientsByUser failed: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 clients, got %d", len(results))
		}
	})
}

func TestOAuthRepo_DeleteClient(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		client := newTestOAuthClient(t, ctx, repo, user)

		if err := repo.DeleteClient(ctx, client.ClientID); err != nil {
			t.Fatalf("DeleteClient failed: %v", err)
		}

		_, err := repo.GetClientByID(ctx, client.ClientID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Authorization Code tests
// ---------------------------------------------------------------------------

func TestOAuthRepo_CreateAuthCode(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		challenge := "abc123challenge-" + uuid.New().String()[:8]
		code := &model.OAuthAuthorizationCode{
			Code:                "authcode-" + uuid.New().String()[:8],
			ClientID:            client.ClientID,
			UserID:              user.ID,
			RedirectURI:         "https://example.com/callback",
			Scope:               "read write",
			CodeChallenge:       &challenge,
			CodeChallengeMethod: "S256",
			ExpiresAt:           time.Now().Add(10 * time.Minute).UTC().Truncate(time.Second),
		}

		if err := repo.CreateAuthCode(ctx, code); err != nil {
			t.Fatalf("CreateAuthCode failed: %v", err)
		}

		if code.Code == "" {
			t.Fatal("expected non-empty code")
		}
		if code.ClientID != client.ClientID {
			t.Fatalf("unexpected client_id: %q", code.ClientID)
		}
		if code.UserID != user.ID {
			t.Fatalf("unexpected user_id: %s", code.UserID)
		}
		if code.Scope != "read write" {
			t.Fatalf("unexpected scope: %q", code.Scope)
		}
		if code.CodeChallenge == nil || *code.CodeChallenge != challenge {
			t.Fatalf("unexpected code_challenge: %v", code.CodeChallenge)
		}
		if code.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
	})
}

func TestOAuthRepo_GetAuthCode(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		codeVal := "get-code-" + uuid.New().String()[:8]
		code := &model.OAuthAuthorizationCode{
			Code:                codeVal,
			ClientID:            client.ClientID,
			UserID:              user.ID,
			RedirectURI:         "https://example.com/callback",
			Scope:               "read",
			CodeChallengeMethod: "S256",
			ExpiresAt:           time.Now().Add(10 * time.Minute).UTC().Truncate(time.Second),
		}
		if err := repo.CreateAuthCode(ctx, code); err != nil {
			t.Fatalf("CreateAuthCode failed: %v", err)
		}

		fetched, err := repo.GetAuthCode(ctx, codeVal)
		if err != nil {
			t.Fatalf("GetAuthCode failed: %v", err)
		}
		if fetched.Code != codeVal {
			t.Fatalf("expected code %q, got %q", codeVal, fetched.Code)
		}
		if fetched.UserID != user.ID {
			t.Fatalf("expected user_id %s, got %s", user.ID, fetched.UserID)
		}
	})
}

func TestOAuthRepo_GetAuthCode_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		_, err := repo.GetAuthCode(ctx, "nonexistent-code-"+uuid.New().String()[:8])
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestOAuthRepo_ConsumeAuthCode(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		codeVal := "consume-" + uuid.New().String()[:8]
		code := &model.OAuthAuthorizationCode{
			Code:                codeVal,
			ClientID:            client.ClientID,
			UserID:              user.ID,
			RedirectURI:         "https://example.com/callback",
			CodeChallengeMethod: "S256",
			ExpiresAt:           time.Now().Add(10 * time.Minute).UTC().Truncate(time.Second),
		}
		if err := repo.CreateAuthCode(ctx, code); err != nil {
			t.Fatalf("CreateAuthCode failed: %v", err)
		}

		if err := repo.ConsumeAuthCode(ctx, codeVal); err != nil {
			t.Fatalf("ConsumeAuthCode failed: %v", err)
		}

		// Code should no longer exist
		_, err := repo.GetAuthCode(ctx, codeVal)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after consume, got %v", err)
		}
	})
}

func TestOAuthRepo_ConsumeAuthCode_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		err := repo.ConsumeAuthCode(ctx, "nonexistent-code-"+uuid.New().String()[:8])
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestOAuthRepo_DeleteExpiredCodes(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		// Create an expired code
		expired := &model.OAuthAuthorizationCode{
			Code:                "expired-" + uuid.New().String()[:8],
			ClientID:            client.ClientID,
			UserID:              user.ID,
			RedirectURI:         "https://example.com/callback",
			CodeChallengeMethod: "S256",
			ExpiresAt:           time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second),
		}
		if err := repo.CreateAuthCode(ctx, expired); err != nil {
			t.Fatalf("CreateAuthCode (expired) failed: %v", err)
		}

		// Create a non-expired code
		valid := &model.OAuthAuthorizationCode{
			Code:                "valid-" + uuid.New().String()[:8],
			ClientID:            client.ClientID,
			UserID:              user.ID,
			RedirectURI:         "https://example.com/callback",
			CodeChallengeMethod: "S256",
			ExpiresAt:           time.Now().Add(1 * time.Hour).UTC().Truncate(time.Second),
		}
		if err := repo.CreateAuthCode(ctx, valid); err != nil {
			t.Fatalf("CreateAuthCode (valid) failed: %v", err)
		}

		deleted, err := repo.DeleteExpiredCodes(ctx)
		if err != nil {
			t.Fatalf("DeleteExpiredCodes failed: %v", err)
		}
		if deleted < 1 {
			t.Fatalf("expected at least 1 deleted, got %d", deleted)
		}

		// Valid code should still exist
		_, err = repo.GetAuthCode(ctx, valid.Code)
		if err != nil {
			t.Fatalf("expected valid code to still exist: %v", err)
		}

		// Expired code should be gone
		_, err = repo.GetAuthCode(ctx, expired.Code)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected expired code to be gone, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Refresh Token tests
// ---------------------------------------------------------------------------

func TestOAuthRepo_CreateRefreshToken(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		expires := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
		token := &model.OAuthRefreshToken{
			TokenHash: "hash-" + uuid.New().String()[:8],
			ClientID:  client.ClientID,
			UserID:    user.ID,
			Scope:     "read",
			ExpiresAt: &expires,
		}

		if err := repo.CreateRefreshToken(ctx, token); err != nil {
			t.Fatalf("CreateRefreshToken failed: %v", err)
		}

		if token.TokenHash == "" {
			t.Fatal("expected non-empty token_hash")
		}
		if token.ClientID != client.ClientID {
			t.Fatalf("unexpected client_id: %q", token.ClientID)
		}
		if token.RevokedAt != nil {
			t.Fatal("expected nil revoked_at")
		}
		if token.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
	})
}

func TestOAuthRepo_CreateRefreshToken_NoExpiry(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		token := &model.OAuthRefreshToken{
			TokenHash: "no-exp-" + uuid.New().String()[:8],
			ClientID:  client.ClientID,
			UserID:    user.ID,
			Scope:     "read",
		}

		if err := repo.CreateRefreshToken(ctx, token); err != nil {
			t.Fatalf("CreateRefreshToken failed: %v", err)
		}

		if token.ExpiresAt != nil {
			t.Fatalf("expected nil expires_at, got %v", token.ExpiresAt)
		}
	})
}

func TestOAuthRepo_GetRefreshToken(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		hash := "get-rt-" + uuid.New().String()[:8]
		token := &model.OAuthRefreshToken{
			TokenHash: hash,
			ClientID:  client.ClientID,
			UserID:    user.ID,
			Scope:     "read write",
		}
		if err := repo.CreateRefreshToken(ctx, token); err != nil {
			t.Fatalf("CreateRefreshToken failed: %v", err)
		}

		fetched, err := repo.GetRefreshToken(ctx, hash)
		if err != nil {
			t.Fatalf("GetRefreshToken failed: %v", err)
		}
		if fetched.TokenHash != hash {
			t.Fatalf("expected hash %q, got %q", hash, fetched.TokenHash)
		}
		if fetched.Scope != "read write" {
			t.Fatalf("expected scope %q, got %q", "read write", fetched.Scope)
		}
	})
}

func TestOAuthRepo_GetRefreshToken_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		_, err := repo.GetRefreshToken(ctx, "nonexistent-hash-"+uuid.New().String()[:8])
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestOAuthRepo_RevokeRefreshToken(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		hash := "revoke-" + uuid.New().String()[:8]
		token := &model.OAuthRefreshToken{
			TokenHash: hash,
			ClientID:  client.ClientID,
			UserID:    user.ID,
			Scope:     "read",
		}
		if err := repo.CreateRefreshToken(ctx, token); err != nil {
			t.Fatalf("CreateRefreshToken failed: %v", err)
		}

		if err := repo.RevokeRefreshToken(ctx, hash); err != nil {
			t.Fatalf("RevokeRefreshToken failed: %v", err)
		}

		fetched, err := repo.GetRefreshToken(ctx, hash)
		if err != nil {
			t.Fatalf("GetRefreshToken after revoke failed: %v", err)
		}
		if fetched.RevokedAt == nil {
			t.Fatal("expected non-nil revoked_at after revoke")
		}
	})
}

func TestOAuthRepo_RevokeRefreshToken_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		err := repo.RevokeRefreshToken(ctx, "nonexistent-hash-"+uuid.New().String()[:8])
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestOAuthRepo_RevokeAllForUser(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)
		client := newTestOAuthClient(t, ctx, repo, user)

		// Create 3 tokens for the user
		for i := 0; i < 3; i++ {
			token := &model.OAuthRefreshToken{
				TokenHash: "revokeall-" + uuid.New().String()[:8],
				ClientID:  client.ClientID,
				UserID:    user.ID,
				Scope:     "read",
			}
			if err := repo.CreateRefreshToken(ctx, token); err != nil {
				t.Fatalf("CreateRefreshToken %d failed: %v", i, err)
			}
		}

		revoked, err := repo.RevokeAllForUser(ctx, user.ID)
		if err != nil {
			t.Fatalf("RevokeAllForUser failed: %v", err)
		}
		if revoked != 3 {
			t.Fatalf("expected 3 revoked, got %d", revoked)
		}

		// Calling again should revoke 0 (already revoked)
		revoked, err = repo.RevokeAllForUser(ctx, user.ID)
		if err != nil {
			t.Fatalf("RevokeAllForUser (second call) failed: %v", err)
		}
		if revoked != 0 {
			t.Fatalf("expected 0 revoked on second call, got %d", revoked)
		}
	})
}

func TestOAuthRepo_RevokeAllForUser_NoTokens(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		revoked, err := repo.RevokeAllForUser(ctx, user.ID)
		if err != nil {
			t.Fatalf("RevokeAllForUser failed: %v", err)
		}
		if revoked != 0 {
			t.Fatalf("expected 0 revoked, got %d", revoked)
		}
	})
}

// ---------------------------------------------------------------------------
// Identity Provider tests
// ---------------------------------------------------------------------------

func TestOAuthRepo_CreateIdP(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)
		user := createTestUser(t, ctx, db)

		suffix := uuid.New().String()[:8]
		issuer := "https://accounts.google.com/" + suffix
		idp := &model.OAuthIdPConfig{
			OrgID:          &user.OrgID,
			ProviderType:   "google",
			ClientID:       "google-client-id-" + suffix,
			ClientSecret:   "google-secret-" + suffix,
			IssuerURL:      &issuer,
			AllowedDomains: []string{"example.com"},
			AutoProvision:  true,
			DefaultRole:    "member",
		}

		if err := repo.CreateIdP(ctx, idp); err != nil {
			t.Fatalf("CreateIdP failed: %v", err)
		}

		if idp.ID == uuid.Nil {
			t.Fatal("expected non-nil ID after create")
		}
		if idp.ProviderType != "google" {
			t.Fatalf("unexpected provider_type: %q", idp.ProviderType)
		}
		if idp.IssuerURL == nil || *idp.IssuerURL != issuer {
			t.Fatalf("unexpected issuer_url: %v", idp.IssuerURL)
		}
		if len(idp.AllowedDomains) != 1 || idp.AllowedDomains[0] != "example.com" {
			t.Fatalf("unexpected allowed_domains: %v", idp.AllowedDomains)
		}
		if !idp.AutoProvision {
			t.Fatal("expected auto_provision true")
		}
		if idp.DefaultRole != "member" {
			t.Fatalf("unexpected default_role: %q", idp.DefaultRole)
		}
		if idp.CreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at")
		}
		if idp.UpdatedAt.IsZero() {
			t.Fatal("expected non-zero updated_at")
		}
	})
}

func TestOAuthRepo_CreateIdP_GeneratesID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		suffix := uuid.New().String()[:8]
		idp := &model.OAuthIdPConfig{
			ProviderType: "github",
			ClientID:     "gh-cid-" + suffix,
			ClientSecret: "gh-sec-" + suffix,
			DefaultRole:  "viewer",
		}
		if err := repo.CreateIdP(ctx, idp); err != nil {
			t.Fatalf("CreateIdP failed: %v", err)
		}
		if idp.ID == uuid.Nil {
			t.Fatal("expected non-nil generated ID")
		}
	})
}

func TestOAuthRepo_CreateIdP_NilDomains(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		suffix := uuid.New().String()[:8]
		idp := &model.OAuthIdPConfig{
			ProviderType: "okta",
			ClientID:     "okta-cid-" + suffix,
			ClientSecret: "okta-sec-" + suffix,
			DefaultRole:  "member",
		}
		if err := repo.CreateIdP(ctx, idp); err != nil {
			t.Fatalf("CreateIdP failed: %v", err)
		}
		if idp.AllowedDomains == nil {
			t.Fatal("expected non-nil allowed_domains")
		}
		if len(idp.AllowedDomains) != 0 {
			t.Fatalf("expected 0 domains, got %d", len(idp.AllowedDomains))
		}
	})
}

func TestOAuthRepo_GetIdPByID(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		suffix := uuid.New().String()[:8]
		idp := &model.OAuthIdPConfig{
			ProviderType: "azure",
			ClientID:     "az-cid-" + suffix,
			ClientSecret: "az-sec-" + suffix,
			DefaultRole:  "member",
		}
		if err := repo.CreateIdP(ctx, idp); err != nil {
			t.Fatalf("CreateIdP failed: %v", err)
		}

		fetched, err := repo.GetIdPByID(ctx, idp.ID)
		if err != nil {
			t.Fatalf("GetIdPByID failed: %v", err)
		}
		if fetched.ID != idp.ID {
			t.Fatalf("expected ID %s, got %s", idp.ID, fetched.ID)
		}
		if fetched.ProviderType != "azure" {
			t.Fatalf("expected provider_type %q, got %q", "azure", fetched.ProviderType)
		}
	})
}

func TestOAuthRepo_GetIdPByID_NotFound(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		_, err := repo.GetIdPByID(ctx, uuid.New())
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}
	})
}

func TestOAuthRepo_ListIdPs(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		for i := 0; i < 3; i++ {
			idp := &model.OAuthIdPConfig{
				ProviderType: "provider-" + uuid.New().String()[:4],
				ClientID:     "cid-" + uuid.New().String()[:8],
				ClientSecret: "sec-" + uuid.New().String()[:8],
				DefaultRole:  "member",
			}
			if err := repo.CreateIdP(ctx, idp); err != nil {
				t.Fatalf("CreateIdP %d failed: %v", i, err)
			}
		}

		results, err := repo.ListIdPs(ctx)
		if err != nil {
			t.Fatalf("ListIdPs failed: %v", err)
		}
		if len(results) < 3 {
			t.Fatalf("expected at least 3 idps, got %d", len(results))
		}
	})
}

func TestOAuthRepo_ListIdPs_Empty(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		// In a shared Postgres DB, other tests may have inserted IdPs,
		// so we just verify the call succeeds without error.
		_, err := repo.ListIdPs(ctx)
		if err != nil {
			t.Fatalf("ListIdPs failed: %v", err)
		}
	})
}

func TestOAuthRepo_DeleteIdP(t *testing.T) {
	forEachDB(t, func(t *testing.T, db DB) {
		ctx := context.Background()
		repo := NewOAuthRepo(db)

		suffix := uuid.New().String()[:8]
		idp := &model.OAuthIdPConfig{
			ProviderType: "delete-me-" + suffix,
			ClientID:     "del-cid-" + suffix,
			ClientSecret: "del-sec-" + suffix,
			DefaultRole:  "member",
		}
		if err := repo.CreateIdP(ctx, idp); err != nil {
			t.Fatalf("CreateIdP failed: %v", err)
		}

		if err := repo.DeleteIdP(ctx, idp.ID); err != nil {
			t.Fatalf("DeleteIdP failed: %v", err)
		}

		_, err := repo.GetIdPByID(ctx, idp.ID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
		}
	})
}
