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

// OrganizationRepo provides CRUD operations for the organizations table.
type OrganizationRepo struct {
	db DB
}

// NewOrganizationRepo creates a new OrganizationRepo backed by the given DB.
func NewOrganizationRepo(db DB) *OrganizationRepo {
	return &OrganizationRepo{db: db}
}

// Create inserts a new organization. ID is generated if zero-valued.
// Settings defaults to `{}` if nil. CreatedAt and UpdatedAt are set by the database defaults.
func (r *OrganizationRepo) Create(ctx context.Context, org *model.Organization) error {
	if org.ID == uuid.Nil {
		org.ID = uuid.New()
	}
	if org.Settings == nil {
		org.Settings = json.RawMessage(`{}`)
	}

	query := `INSERT INTO organizations (id, namespace_id, name, slug, settings)
		VALUES (?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO organizations (id, namespace_id, name, slug, settings)
			VALUES ($1, $2, $3, $4, $5)`
	}

	_, err := r.db.Exec(ctx, query,
		org.ID.String(), org.NamespaceID.String(), org.Name, org.Slug, string(org.Settings),
	)
	if err != nil {
		return fmt.Errorf("organization create: %w", err)
	}

	return r.reload(ctx, org)
}

// GetByID returns an organization by its UUID.
func (r *OrganizationRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Organization, error) {
	query := `SELECT id, namespace_id, name, slug, settings, created_at, updated_at
		FROM organizations WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, namespace_id, name, slug, settings, created_at, updated_at
			FROM organizations WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanOrganization(row)
}

// GetBySlug returns an organization by its slug.
func (r *OrganizationRepo) GetBySlug(ctx context.Context, slug string) (*model.Organization, error) {
	query := `SELECT id, namespace_id, name, slug, settings, created_at, updated_at
		FROM organizations WHERE slug = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, namespace_id, name, slug, settings, created_at, updated_at
			FROM organizations WHERE slug = $1`
	}

	row := r.db.QueryRow(ctx, query, slug)
	return r.scanOrganization(row)
}

// GetByNamespaceID returns an organization by its namespace UUID.
func (r *OrganizationRepo) GetByNamespaceID(ctx context.Context, namespaceID uuid.UUID) (*model.Organization, error) {
	query := `SELECT id, namespace_id, name, slug, settings, created_at, updated_at
		FROM organizations WHERE namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT id, namespace_id, name, slug, settings, created_at, updated_at
			FROM organizations WHERE namespace_id = $1`
	}

	row := r.db.QueryRow(ctx, query, namespaceID.String())
	return r.scanOrganization(row)
}

// List returns all organizations ordered by name.
func (r *OrganizationRepo) List(ctx context.Context) ([]model.Organization, error) {
	query := `SELECT id, namespace_id, name, slug, settings, created_at, updated_at
		FROM organizations ORDER BY name`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("organization list: %w", err)
	}
	defer rows.Close()

	result := []model.Organization{}
	for rows.Next() {
		org, err := r.scanOrganizationFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *org)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("organization list iteration: %w", err)
	}
	return result, nil
}

// Update updates an organization's name, slug, settings, and updated_at.
func (r *OrganizationRepo) Update(ctx context.Context, org *model.Organization) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE organizations SET name = ?, slug = ?, settings = ?, updated_at = ?
		WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE organizations SET name = $1, slug = $2, settings = $3, updated_at = $4
			WHERE id = $5`
	}

	if org.Settings == nil {
		org.Settings = json.RawMessage(`{}`)
	}

	_, err := r.db.Exec(ctx, query,
		org.Name, org.Slug, string(org.Settings), now, org.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("organization update: %w", err)
	}

	return r.reload(ctx, org)
}

// Delete hard-deletes an organization by ID.
func (r *OrganizationRepo) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM organizations WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM organizations WHERE id = $1`
	}

	_, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("organization delete: %w", err)
	}
	return nil
}

// reload fetches the organization by ID and populates the struct in place.
func (r *OrganizationRepo) reload(ctx context.Context, org *model.Organization) error {
	fetched, err := r.GetByID(ctx, org.ID)
	if err != nil {
		return fmt.Errorf("organization reload: %w", err)
	}
	*org = *fetched
	return nil
}

// scanOrganization scans a single row into a model.Organization.
func (r *OrganizationRepo) scanOrganization(row *sql.Row) (*model.Organization, error) {
	var org model.Organization
	var idStr, namespaceIDStr string
	var settingsStr string
	var createdAtStr, updatedAtStr string

	err := row.Scan(
		&idStr, &namespaceIDStr, &org.Name, &org.Slug,
		&settingsStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("organization scan parse id: %w", err)
	}
	org.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("organization scan parse namespace_id: %w", err)
	}
	org.NamespaceID = nsID

	org.Settings = json.RawMessage(settingsStr)

	org.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("organization scan parse created_at: %w", err)
	}
	org.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("organization scan parse updated_at: %w", err)
	}

	return &org, nil
}

// scanOrganizationFromRows scans the current row from sql.Rows into a model.Organization.
func (r *OrganizationRepo) scanOrganizationFromRows(rows *sql.Rows) (*model.Organization, error) {
	var org model.Organization
	var idStr, namespaceIDStr string
	var settingsStr string
	var createdAtStr, updatedAtStr string

	err := rows.Scan(
		&idStr, &namespaceIDStr, &org.Name, &org.Slug,
		&settingsStr, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("organization scan rows: %w", err)
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("organization scan rows parse id: %w", err)
	}
	org.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("organization scan rows parse namespace_id: %w", err)
	}
	org.NamespaceID = nsID

	org.Settings = json.RawMessage(settingsStr)

	org.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("organization scan rows parse created_at: %w", err)
	}
	org.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("organization scan rows parse updated_at: %w", err)
	}

	return &org, nil
}
