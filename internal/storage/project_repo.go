package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// ProjectRepo provides CRUD operations for the projects table.
type ProjectRepo struct {
	db DB
}

// NewProjectRepo creates a new ProjectRepo backed by the given DB.
func NewProjectRepo(db DB) *ProjectRepo {
	return &ProjectRepo{db: db}
}

// Create inserts a new project. ID is generated if zero-valued.
// Settings defaults to `{}` if nil. DefaultTags defaults to `[]` if nil.
func (r *ProjectRepo) Create(ctx context.Context, project *model.Project) error {
	if project.ID == uuid.Nil {
		project.ID = uuid.New()
	}
	if project.Settings == nil {
		project.Settings = json.RawMessage(`{}`)
	}
	if project.DefaultTags == nil {
		project.DefaultTags = []string{}
	}

	tagsVal := encodeStringArray(r.db.Backend(), project.DefaultTags)

	query := `INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	}

	_, err := r.db.Exec(ctx, query,
		project.ID.String(), project.NamespaceID.String(), project.OwnerNamespaceID.String(),
		project.Name, project.Slug, project.Description,
		tagsVal, string(project.Settings),
	)
	if err != nil {
		return fmt.Errorf("project create: %w", err)
	}

	return r.reload(ctx, project)
}

// GetByID returns a project by its UUID.
func (r *ProjectRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error) {
	query := `SELECT id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings, created_at, updated_at
		FROM projects WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings, created_at, updated_at
			FROM projects WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanProject(row)
}

// GetBySlug returns a project by its owner_namespace_id and slug (unique constraint).
func (r *ProjectRepo) GetBySlug(ctx context.Context, ownerNamespaceID uuid.UUID, slug string) (*model.Project, error) {
	query := `SELECT id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings, created_at, updated_at
		FROM projects WHERE owner_namespace_id = ? AND slug = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings, created_at, updated_at
			FROM projects WHERE owner_namespace_id = $1 AND slug = $2`
	}

	row := r.db.QueryRow(ctx, query, ownerNamespaceID.String(), slug)
	return r.scanProject(row)
}

// ListByUser returns all projects owned by the given namespace, ordered by name.
func (r *ProjectRepo) ListByUser(ctx context.Context, ownerNamespaceID uuid.UUID) ([]model.Project, error) {
	query := `SELECT id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings, created_at, updated_at
		FROM projects WHERE owner_namespace_id = ? ORDER BY name`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, namespace_id, owner_namespace_id, name, slug, description, default_tags, settings, created_at, updated_at
			FROM projects WHERE owner_namespace_id = $1 ORDER BY name`
	}

	rows, err := r.db.Query(ctx, query, ownerNamespaceID.String())
	if err != nil {
		return nil, fmt.Errorf("project list by user: %w", err)
	}
	defer rows.Close()

	var result []model.Project
	for rows.Next() {
		p, err := r.scanProjectFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("project list by user iteration: %w", err)
	}
	return result, nil
}

// Update updates a project's mutable fields: name, slug, description, default_tags, settings.
func (r *ProjectRepo) Update(ctx context.Context, project *model.Project) error {
	now := time.Now().UTC().Format(time.RFC3339)

	if project.Settings == nil {
		project.Settings = json.RawMessage(`{}`)
	}
	if project.DefaultTags == nil {
		project.DefaultTags = []string{}
	}

	tagsVal := encodeStringArray(r.db.Backend(), project.DefaultTags)

	query := `UPDATE projects SET name = ?, slug = ?, description = ?, default_tags = ?, settings = ?, updated_at = ?
		WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE projects SET name = $1, slug = $2, description = $3, default_tags = $4, settings = $5, updated_at = $6
			WHERE id = $7`
	}

	_, err := r.db.Exec(ctx, query,
		project.Name, project.Slug, project.Description,
		tagsVal, string(project.Settings), now, project.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("project update: %w", err)
	}

	return r.reload(ctx, project)
}

// Delete hard-deletes a project by ID.
func (r *ProjectRepo) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM projects WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM projects WHERE id = $1`
	}

	_, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("project delete: %w", err)
	}
	return nil
}

// AutoCreateUnderUser creates a project with default settings under a user's namespace.
// It also creates a child namespace for the project. If a project with the given slug
// already exists under the user's namespace, it returns the existing project.
func (r *ProjectRepo) AutoCreateUnderUser(ctx context.Context, nsRepo *NamespaceRepo, userNamespaceID uuid.UUID, slug string) (*model.Project, error) {
	// Check if a project with this slug already exists under this user namespace.
	existing, err := r.GetBySlug(ctx, userNamespaceID, slug)
	if err == nil {
		return existing, nil
	}

	// Look up the user's namespace to build the child path.
	userNS, err := nsRepo.GetByID(ctx, userNamespaceID)
	if err != nil {
		return nil, fmt.Errorf("project auto create resolve user namespace: %w", err)
	}

	// Create a child namespace for the project.
	projectNSID := uuid.New()
	projectNS := &model.Namespace{
		ID:       projectNSID,
		Name:     slug,
		Slug:     slug,
		Kind:     "project",
		ParentID: &userNamespaceID,
		Path:     userNS.Path + "/" + slug,
		Depth:    userNS.Depth + 1,
	}
	if err := nsRepo.Create(ctx, projectNS); err != nil {
		return nil, fmt.Errorf("project auto create namespace: %w", err)
	}

	// Create the project.
	project := &model.Project{
		NamespaceID:      projectNSID,
		OwnerNamespaceID: userNamespaceID,
		Name:             slug,
		Slug:             slug,
		Description:      "",
		DefaultTags:      []string{},
		Settings:         json.RawMessage(`{}`),
	}
	if err := r.Create(ctx, project); err != nil {
		return nil, fmt.Errorf("project auto create: %w", err)
	}

	return project, nil
}

// reload fetches the project by ID and populates the struct in place.
func (r *ProjectRepo) reload(ctx context.Context, project *model.Project) error {
	fetched, err := r.GetByID(ctx, project.ID)
	if err != nil {
		return fmt.Errorf("project reload: %w", err)
	}
	*project = *fetched
	return nil
}

// scanProject scans a single row into a model.Project.
func (r *ProjectRepo) scanProject(row *sql.Row) (*model.Project, error) {
	var project model.Project
	var idStr, namespaceIDStr, ownerNamespaceIDStr string
	var defaultTagsStr, settingsStr string
	var createdAtStr, updatedAtStr string

	err := row.Scan(
		&idStr, &namespaceIDStr, &ownerNamespaceIDStr,
		&project.Name, &project.Slug, &project.Description,
		&defaultTagsStr, &settingsStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("project scan parse id: %w", err)
	}
	project.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("project scan parse namespace_id: %w", err)
	}
	project.NamespaceID = nsID

	ownerNSID, err := uuid.Parse(ownerNamespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("project scan parse owner_namespace_id: %w", err)
	}
	project.OwnerNamespaceID = ownerNSID

	tags, err := decodeStringArray(r.db.Backend(), defaultTagsStr)
	if err != nil {
		return nil, fmt.Errorf("project scan parse default_tags: %w", err)
	}
	project.DefaultTags = tags

	project.Settings = json.RawMessage(settingsStr)

	project.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("project scan parse created_at: %w", err)
	}
	project.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("project scan parse updated_at: %w", err)
	}

	return &project, nil
}

// scanProjectFromRows scans the current row from sql.Rows into a model.Project.
func (r *ProjectRepo) scanProjectFromRows(rows *sql.Rows) (*model.Project, error) {
	var project model.Project
	var idStr, namespaceIDStr, ownerNamespaceIDStr string
	var defaultTagsStr, settingsStr string
	var createdAtStr, updatedAtStr string

	err := rows.Scan(
		&idStr, &namespaceIDStr, &ownerNamespaceIDStr,
		&project.Name, &project.Slug, &project.Description,
		&defaultTagsStr, &settingsStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("project scan rows: %w", err)
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("project scan rows parse id: %w", err)
	}
	project.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("project scan rows parse namespace_id: %w", err)
	}
	project.NamespaceID = nsID

	ownerNSID, err := uuid.Parse(ownerNamespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("project scan rows parse owner_namespace_id: %w", err)
	}
	project.OwnerNamespaceID = ownerNSID

	tags, err := decodeStringArray(r.db.Backend(), defaultTagsStr)
	if err != nil {
		return nil, fmt.Errorf("project scan rows parse default_tags: %w", err)
	}
	project.DefaultTags = tags

	project.Settings = json.RawMessage(settingsStr)

	project.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("project scan rows parse created_at: %w", err)
	}
	project.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("project scan rows parse updated_at: %w", err)
	}

	return &project, nil
}
