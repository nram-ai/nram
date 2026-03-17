package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// OrgAdminStore implements api.OrgStore by wrapping OrganizationRepo and NamespaceRepo.
type OrgAdminStore struct {
	orgRepo *storage.OrganizationRepo
	nsRepo  *storage.NamespaceRepo
}

// NewOrgAdminStore creates a new OrgAdminStore.
func NewOrgAdminStore(orgRepo *storage.OrganizationRepo, nsRepo *storage.NamespaceRepo) *OrgAdminStore {
	return &OrgAdminStore{orgRepo: orgRepo, nsRepo: nsRepo}
}

func (s *OrgAdminStore) CountOrgs(ctx context.Context) (int, error) {
	return s.orgRepo.Count(ctx)
}

func (s *OrgAdminStore) ListOrgs(ctx context.Context, limit, offset int) ([]model.Organization, error) {
	return s.orgRepo.ListPaged(ctx, limit, offset)
}

func (s *OrgAdminStore) CreateOrg(ctx context.Context, name, slug string) (*model.Organization, error) {
	// Create the org namespace first.
	nsID := uuid.New()
	rootID := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	ns := &model.Namespace{
		ID:       nsID,
		Name:     name,
		Slug:     slug,
		Kind:     "org",
		ParentID: &rootID,
		Path:     slug,
		Depth:    1,
	}
	if err := s.nsRepo.Create(ctx, ns); err != nil {
		return nil, fmt.Errorf("create org namespace: %w", err)
	}

	org := &model.Organization{
		NamespaceID: nsID,
		Name:        name,
		Slug:        slug,
		Settings:    json.RawMessage(`{}`),
	}
	if err := s.orgRepo.Create(ctx, org); err != nil {
		return nil, fmt.Errorf("create org: %w", err)
	}
	return org, nil
}

func (s *OrgAdminStore) GetOrg(ctx context.Context, id uuid.UUID) (*model.Organization, error) {
	return s.orgRepo.GetByID(ctx, id)
}

func (s *OrgAdminStore) UpdateOrg(ctx context.Context, id uuid.UUID, name, slug string, settings json.RawMessage) (*model.Organization, error) {
	org, err := s.orgRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if name != "" {
		org.Name = name
	}
	if slug != "" {
		org.Slug = slug
	}
	if settings != nil {
		org.Settings = settings
	}

	if err := s.orgRepo.Update(ctx, org); err != nil {
		return nil, err
	}
	return org, nil
}

func (s *OrgAdminStore) DeleteOrg(ctx context.Context, id uuid.UUID) error {
	return s.orgRepo.Delete(ctx, id)
}
