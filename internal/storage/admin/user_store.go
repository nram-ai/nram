package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// UserAdminStore implements api.UserAdminStore by wrapping UserRepo, APIKeyRepo,
// NamespaceRepo, and OrganizationRepo.
type UserAdminStore struct {
	userRepo   *storage.UserRepo
	apiKeyRepo *storage.APIKeyRepo
	nsRepo     *storage.NamespaceRepo
	orgRepo    *storage.OrganizationRepo
}

// NewUserAdminStore creates a new UserAdminStore.
func NewUserAdminStore(
	userRepo *storage.UserRepo,
	apiKeyRepo *storage.APIKeyRepo,
	nsRepo *storage.NamespaceRepo,
	orgRepo *storage.OrganizationRepo,
) *UserAdminStore {
	return &UserAdminStore{
		userRepo:   userRepo,
		apiKeyRepo: apiKeyRepo,
		nsRepo:     nsRepo,
		orgRepo:    orgRepo,
	}
}

func (s *UserAdminStore) ListUsers(ctx context.Context) ([]model.User, error) {
	return s.userRepo.ListAll(ctx)
}

func (s *UserAdminStore) CreateUser(ctx context.Context, email, displayName, password, role string, orgID uuid.UUID) (*model.User, error) {
	hash, err := storage.HashPassword(password)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	// Resolve the org's namespace path when an org is specified.
	var orgNSPath string
	if orgID != uuid.Nil {
		org, err := s.orgRepo.GetByID(ctx, orgID)
		if err != nil {
			return nil, fmt.Errorf("resolve org: %w", err)
		}
		orgNS, err := s.nsRepo.GetByID(ctx, org.NamespaceID)
		if err != nil {
			return nil, fmt.Errorf("resolve org namespace: %w", err)
		}
		orgNSPath = orgNS.Path
	}

	user := &model.User{
		Email:        email,
		DisplayName:  displayName,
		PasswordHash: &hash,
		OrgID:        orgID,
		Role:         role,
		Settings:     json.RawMessage(`{}`),
	}
	if err := s.userRepo.Create(ctx, user, s.nsRepo, orgNSPath); err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}
	return user, nil
}

func (s *UserAdminStore) GetUser(ctx context.Context, id uuid.UUID) (*model.User, error) {
	return s.userRepo.GetByID(ctx, id)
}

func (s *UserAdminStore) UpdateUser(ctx context.Context, id uuid.UUID, displayName, role string, settings json.RawMessage) (*model.User, error) {
	user, err := s.userRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if displayName != "" {
		user.DisplayName = displayName
	}
	if role != "" {
		user.Role = role
	}
	if settings != nil {
		user.Settings = settings
	}

	if err := s.userRepo.Update(ctx, user); err != nil {
		return nil, err
	}
	return user, nil
}

func (s *UserAdminStore) DeleteUser(ctx context.Context, id uuid.UUID) error {
	return s.userRepo.Delete(ctx, id)
}

func (s *UserAdminStore) CountAdmins(ctx context.Context) (int, error) {
	return s.userRepo.CountAdmins(ctx)
}

func (s *UserAdminStore) ListAPIKeys(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error) {
	return s.apiKeyRepo.ListByUser(ctx, userID)
}

func (s *UserAdminStore) GenerateAPIKey(ctx context.Context, userID uuid.UUID, name string, scopes []uuid.UUID, expiresAt *time.Time) (*model.APIKey, string, error) {
	key := &model.APIKey{
		UserID:    userID,
		Name:      name,
		Scopes:    scopes,
		ExpiresAt: expiresAt,
	}
	rawKey, err := s.apiKeyRepo.Create(ctx, key)
	if err != nil {
		return nil, "", fmt.Errorf("generate api key: %w", err)
	}
	return key, rawKey, nil
}

func (s *UserAdminStore) RevokeAPIKey(ctx context.Context, keyID uuid.UUID) error {
	return s.apiKeyRepo.Revoke(ctx, keyID)
}
