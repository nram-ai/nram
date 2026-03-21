package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

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

func (s *ProjectAdminStore) DeleteProject(ctx context.Context, id uuid.UUID) error {
	return s.projectRepo.Delete(ctx, id)
}

// CountProjectsByOrg returns the number of projects under the given organization's namespace.
func (s *ProjectAdminStore) CountProjectsByOrg(ctx context.Context, orgID uuid.UUID) (int, error) {
	query := `SELECT COUNT(*) FROM projects p
		JOIN namespaces pn ON p.namespace_id = pn.id
		WHERE pn.path LIKE (SELECT n.path || '/%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)`
	if s.db.Backend() == storage.BackendPostgres {
		query = `SELECT COUNT(*) FROM projects p
			JOIN namespaces pn ON p.namespace_id = pn.id
			WHERE pn.path LIKE (SELECT n.path || '/' || '%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)`
	}
	row := s.db.QueryRow(ctx, query, orgID.String())
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("project count by org: %w", err)
	}
	return count, nil
}

// ListProjectsByOrg returns projects under the given organization's namespace with pagination.
func (s *ProjectAdminStore) ListProjectsByOrg(ctx context.Context, orgID uuid.UUID, limit, offset int) ([]model.Project, error) {
	query := `SELECT p.id, p.namespace_id, p.owner_namespace_id, p.name, p.slug,
		COALESCE(pn.path, '') AS path,
		p.description, p.default_tags, p.settings,
		COALESCE((SELECT COUNT(*) FROM memories m WHERE m.namespace_id = p.namespace_id AND m.deleted_at IS NULL), 0),
		COALESCE((SELECT COUNT(*) FROM entities e WHERE e.namespace_id = p.namespace_id), 0),
		u.id AS owner_id, u.email AS owner_email,
		org.id AS org_id, org.name AS org_name,
		p.created_at, p.updated_at
		FROM projects p
		JOIN namespaces pn ON p.namespace_id = pn.id
		LEFT JOIN users u ON u.namespace_id = p.owner_namespace_id
		LEFT JOIN organizations org ON org.id = u.org_id
		WHERE pn.path LIKE (SELECT n.path || '/%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?)
		ORDER BY p.name
		LIMIT ? OFFSET ?`
	if s.db.Backend() == storage.BackendPostgres {
		query = `SELECT p.id, p.namespace_id, p.owner_namespace_id, p.name, p.slug,
			COALESCE(pn.path, '') AS path,
			p.description, p.default_tags, p.settings,
			COALESCE((SELECT COUNT(*) FROM memories m WHERE m.namespace_id = p.namespace_id AND m.deleted_at IS NULL), 0),
			COALESCE((SELECT COUNT(*) FROM entities e WHERE e.namespace_id = p.namespace_id), 0),
			u.id AS owner_id, u.email AS owner_email,
			org.id AS org_id, org.name AS org_name,
			p.created_at, p.updated_at
			FROM projects p
			JOIN namespaces pn ON p.namespace_id = pn.id
			LEFT JOIN users u ON u.namespace_id = p.owner_namespace_id
			LEFT JOIN organizations org ON org.id = u.org_id
			WHERE pn.path LIKE (SELECT n.path || '/' || '%' FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1)
			ORDER BY p.name
			LIMIT $2 OFFSET $3`
	}
	rows, err := s.db.Query(ctx, query, orgID.String(), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("project list by org: %w", err)
	}
	defer rows.Close()

	var projects []model.Project
	for rows.Next() {
		var p model.Project
		var idStr, nsIDStr, ownerNSIDStr string
		var tagsStr, settingsStr string
		var ownerIDStr, ownerEmail sql.NullString
		var orgIDStr, orgName sql.NullString
		var createdAtStr, updatedAtStr string
		if err := rows.Scan(&idStr, &nsIDStr, &ownerNSIDStr, &p.Name, &p.Slug, &p.Path,
			&p.Description, &tagsStr, &settingsStr,
			&p.MemoryCount, &p.EntityCount,
			&ownerIDStr, &ownerEmail, &orgIDStr, &orgName,
			&createdAtStr, &updatedAtStr); err != nil {
			return nil, fmt.Errorf("project list by org scan: %w", err)
		}
		parsedID, err := uuid.Parse(idStr)
		if err != nil {
			return nil, fmt.Errorf("project list by org parse id: %w", err)
		}
		p.ID = parsedID
		nsID, err := uuid.Parse(nsIDStr)
		if err != nil {
			return nil, fmt.Errorf("project list by org parse namespace_id: %w", err)
		}
		p.NamespaceID = nsID
		ownerNSID, err := uuid.Parse(ownerNSIDStr)
		if err != nil {
			return nil, fmt.Errorf("project list by org parse owner_namespace_id: %w", err)
		}
		p.OwnerNamespaceID = ownerNSID
		if err := json.Unmarshal([]byte(tagsStr), &p.DefaultTags); err != nil {
			return nil, fmt.Errorf("project list by org parse default_tags: %w", err)
		}
		if p.DefaultTags == nil {
			p.DefaultTags = []string{}
		}
		p.Settings = json.RawMessage(settingsStr)
		if ownerIDStr.Valid && ownerEmail.Valid {
			oID, parseErr := uuid.Parse(ownerIDStr.String)
			if parseErr == nil {
				p.Owner = &model.ProjectOwner{ID: oID, Email: ownerEmail.String}
			}
		}
		if orgIDStr.Valid && orgName.Valid {
			oID, parseErr := uuid.Parse(orgIDStr.String)
			if parseErr == nil {
				p.Organization = &model.ProjectOrg{ID: oID, Name: orgName.String}
			}
		}
		createdAt, err := time.Parse(time.RFC3339, createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("project list by org parse created_at: %w", err)
		}
		p.CreatedAt = createdAt
		updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)
		if err != nil {
			return nil, fmt.Errorf("project list by org parse updated_at: %w", err)
		}
		p.UpdatedAt = updatedAt
		projects = append(projects, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("project list by org iteration: %w", err)
	}
	if projects == nil {
		projects = []model.Project{}
	}
	return projects, nil
}
