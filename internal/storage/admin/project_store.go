package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// ProjectAdminStore implements api.ProjectAdminStore by wrapping ProjectRepo and NamespaceRepo.
type ProjectAdminStore struct {
	db          storage.DB
	projectRepo *storage.ProjectRepo
	nsRepo      *storage.NamespaceRepo
}

// NewProjectAdminStore creates a new ProjectAdminStore.
func NewProjectAdminStore(db storage.DB, projectRepo *storage.ProjectRepo, nsRepo *storage.NamespaceRepo) *ProjectAdminStore {
	return &ProjectAdminStore{db: db, projectRepo: projectRepo, nsRepo: nsRepo}
}

func (s *ProjectAdminStore) CountProjects(ctx context.Context) (int, error) {
	return s.projectRepo.CountAll(ctx)
}

func (s *ProjectAdminStore) ListProjects(ctx context.Context, limit, offset int) ([]model.Project, error) {
	return s.projectRepo.ListAllPaged(ctx, limit, offset)
}

func (s *ProjectAdminStore) CreateProject(ctx context.Context, name, slug, description string, ownerNamespaceID uuid.UUID, defaultTags []string, settings json.RawMessage) (*model.Project, error) {
	// Look up the owner namespace to build the child path.
	ownerNS, err := s.nsRepo.GetByID(ctx, ownerNamespaceID)
	if err != nil {
		return nil, fmt.Errorf("resolve owner namespace: %w", err)
	}

	// Create a child namespace for the project.
	projectNSID := uuid.New()
	projectNS := &model.Namespace{
		ID:       projectNSID,
		Name:     name,
		Slug:     slug,
		Kind:     "project",
		ParentID: &ownerNamespaceID,
		Path:     ownerNS.Path + "/" + slug,
		Depth:    ownerNS.Depth + 1,
	}
	if err := s.nsRepo.Create(ctx, projectNS); err != nil {
		return nil, fmt.Errorf("create project namespace: %w", err)
	}

	if defaultTags == nil {
		defaultTags = []string{}
	}
	if settings == nil {
		settings = json.RawMessage(`{}`)
	}

	project := &model.Project{
		NamespaceID:      projectNSID,
		OwnerNamespaceID: ownerNamespaceID,
		Name:             name,
		Slug:             slug,
		Description:      description,
		DefaultTags:      defaultTags,
		Settings:         settings,
	}
	if err := s.projectRepo.Create(ctx, project); err != nil {
		return nil, fmt.Errorf("create project: %w", err)
	}
	return project, nil
}

func (s *ProjectAdminStore) GetProject(ctx context.Context, id uuid.UUID) (*model.Project, error) {
	return s.projectRepo.GetByID(ctx, id)
}

func (s *ProjectAdminStore) UpdateProject(ctx context.Context, id uuid.UUID, name, slug, description string, defaultTags []string, settings json.RawMessage) (*model.Project, error) {
	project, err := s.projectRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if name != "" {
		project.Name = name
	}
	if slug != "" {
		project.Slug = slug
	}
	project.Description = description
	if defaultTags != nil {
		project.DefaultTags = defaultTags
	}
	if settings != nil {
		project.Settings = settings
	}

	if err := s.projectRepo.Update(ctx, project); err != nil {
		return nil, err
	}
	return project, nil
}


