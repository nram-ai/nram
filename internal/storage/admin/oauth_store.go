package admin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// OAuthAdminStore implements api.OAuthAdminStore by wrapping OAuthRepo.
type OAuthAdminStore struct {
	oauthRepo *storage.OAuthRepo
}

// NewOAuthAdminStore creates a new OAuthAdminStore.
func NewOAuthAdminStore(oauthRepo *storage.OAuthRepo) *OAuthAdminStore {
	return &OAuthAdminStore{oauthRepo: oauthRepo}
}

func (s *OAuthAdminStore) ListAllClients(ctx context.Context) ([]model.OAuthClient, error) {
	return s.oauthRepo.ListAllClients(ctx)
}

func (s *OAuthAdminStore) CreateClient(ctx context.Context, name string, redirectURIs []string, clientType string) (*model.OAuthClient, string, error) {
	clientID := generateRandomHex(16)
	var clientSecret *string
	var plainSecret string
	if clientType == "confidential" {
		plainSecret = generateRandomHex(32)
		clientSecret = &plainSecret
	}

	client := &model.OAuthClient{
		ClientID:       clientID,
		ClientSecret:   clientSecret,
		Name:           name,
		RedirectURIs:   redirectURIs,
		GrantTypes:     []string{"authorization_code", "refresh_token"},
		AutoRegistered: false,
	}

	if err := s.oauthRepo.CreateClient(ctx, client); err != nil {
		return nil, "", fmt.Errorf("create oauth client: %w", err)
	}

	return client, plainSecret, nil
}

func (s *OAuthAdminStore) DeleteClient(ctx context.Context, id uuid.UUID) error {
	return s.oauthRepo.DeleteClientByPK(ctx, id)
}

func (s *OAuthAdminStore) ListIdPs(ctx context.Context) ([]model.OAuthIdPConfig, error) {
	return s.oauthRepo.ListIdPs(ctx)
}

func (s *OAuthAdminStore) CreateIdP(ctx context.Context, req api.CreateIdPRequest) (*model.OAuthIdPConfig, error) {
	idp := &model.OAuthIdPConfig{
		ProviderType:   req.ProviderType,
		ClientID:       req.ClientID,
		ClientSecret:   req.ClientSecret,
		IssuerURL:      req.IssuerURL,
		AllowedDomains: req.AllowedDomains,
		AutoProvision:  req.AutoProvision,
		DefaultRole:    "member",
	}

	if req.OrgID != "" {
		oid, err := uuid.Parse(req.OrgID)
		if err != nil {
			return nil, fmt.Errorf("invalid org_id: %w", err)
		}
		idp.OrgID = &oid
	}

	if err := s.oauthRepo.CreateIdP(ctx, idp); err != nil {
		return nil, fmt.Errorf("create idp: %w", err)
	}

	return idp, nil
}

func (s *OAuthAdminStore) DeleteIdP(ctx context.Context, id uuid.UUID) error {
	return s.oauthRepo.DeleteIdP(ctx, id)
}

// generateRandomHex returns a cryptographically random hex string of n bytes.
func generateRandomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
