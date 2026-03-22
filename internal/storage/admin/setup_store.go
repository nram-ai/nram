package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// SetupStore implements api.SetupStore. It creates the initial org, user, namespace
// hierarchy, and API key during the setup wizard.
type SetupStore struct {
	userRepo    *storage.UserRepo
	nsRepo      *storage.NamespaceRepo
	orgRepo     *storage.OrganizationRepo
	apiKeyRepo  *storage.APIKeyRepo
	projectRepo *storage.ProjectRepo
	db          storage.DB
}

// NewSetupStore creates a new SetupStore.
func NewSetupStore(
	userRepo *storage.UserRepo,
	nsRepo *storage.NamespaceRepo,
	orgRepo *storage.OrganizationRepo,
	apiKeyRepo *storage.APIKeyRepo,
	projectRepo *storage.ProjectRepo,
	db storage.DB,
) *SetupStore {
	return &SetupStore{
		userRepo:    userRepo,
		nsRepo:      nsRepo,
		orgRepo:     orgRepo,
		apiKeyRepo:  apiKeyRepo,
		projectRepo: projectRepo,
		db:          db,
	}
}

func (s *SetupStore) IsSetupComplete(ctx context.Context) (bool, error) {
	val, err := storage.GetSystemMeta(ctx, s.db, "setup_complete")
	if err != nil {
		return false, fmt.Errorf("check setup complete: %w", err)
	}
	if val == "true" {
		return true, nil
	}

	// Fallback for databases upgraded before the system_meta flag existed:
	// if no flag is set but admin users exist, treat setup as complete.
	count, err := s.userRepo.CountAdmins(ctx)
	if err != nil {
		return false, fmt.Errorf("check setup complete (fallback): %w", err)
	}
	return count > 0, nil
}

func (s *SetupStore) CompleteSetup(ctx context.Context, email, password string) (*model.User, string, error) {
	// Hash password.
	hash, err := storage.HashPassword(password)
	if err != nil {
		return nil, "", fmt.Errorf("setup hash password: %w", err)
	}

	// Create root namespace (if it doesn't exist).
	rootID := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	rootNS := &model.Namespace{
		ID:    rootID,
		Name:  "root",
		Slug:  "root",
		Kind:  "root",
		Path:  "root",
		Depth: 0,
	}
	_, _, err = s.nsRepo.CreateIfNotExists(ctx, rootNS)
	if err != nil {
		return nil, "", fmt.Errorf("setup create root namespace: %w", err)
	}

	// Create default org namespace.
	orgNSID := uuid.New()
	orgSlug := "default"
	orgNS := &model.Namespace{
		ID:       orgNSID,
		Name:     "Default Organization",
		Slug:     orgSlug,
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgSlug,
		Depth:    1,
	}
	if err := s.nsRepo.Create(ctx, orgNS); err != nil {
		return nil, "", fmt.Errorf("setup create org namespace: %w", err)
	}

	// Create default org.
	org := &model.Organization{
		NamespaceID: orgNSID,
		Name:        "Default Organization",
		Slug:        orgSlug,
		Settings:    json.RawMessage(`{}`),
	}
	if err := s.orgRepo.Create(ctx, org); err != nil {
		return nil, "", fmt.Errorf("setup create org: %w", err)
	}

	// Create admin user (UserRepo.Create also creates the user namespace).
	user := &model.User{
		Email:        email,
		DisplayName:  "Administrator",
		PasswordHash: &hash,
		OrgID:        org.ID,
		Role:         "administrator",
		Settings:     json.RawMessage(`{}`),
	}
	if err := s.userRepo.Create(ctx, user, s.nsRepo, s.projectRepo, orgSlug); err != nil {
		return nil, "", fmt.Errorf("setup create user: %w", err)
	}

	// Create initial API key.
	apiKey := &model.APIKey{
		UserID: user.ID,
		Name:   "Setup Key",
	}
	rawKey, err := s.apiKeyRepo.Create(ctx, apiKey)
	if err != nil {
		return nil, "", fmt.Errorf("setup create api key: %w", err)
	}

	// Mark setup as complete in system_meta.
	if err := storage.SetSystemMeta(ctx, s.db, "setup_complete", "true"); err != nil {
		return nil, "", fmt.Errorf("setup set setup_complete flag: %w", err)
	}
	if err := storage.SetSystemMeta(ctx, s.db, "storage_backend", s.db.Backend()); err != nil {
		return nil, "", fmt.Errorf("setup set storage_backend: %w", err)
	}

	return user, rawKey, nil
}

func (s *SetupStore) Backend() string {
	return s.db.Backend()
}
