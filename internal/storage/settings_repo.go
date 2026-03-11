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

// scopeOrder defines the cascade resolution order from most specific to least specific.
// When looking up a setting, we try each scope in order until we find a match.
var scopeOrder = []string{"project", "user", "org", "global"}

// SettingsRepo provides CRUD operations for the settings table.
type SettingsRepo struct {
	db DB
}

// NewSettingsRepo creates a new SettingsRepo backed by the given DB.
func NewSettingsRepo(db DB) *SettingsRepo {
	return &SettingsRepo{db: db}
}

// Get retrieves a setting by key and scope. If the setting is not found at the given
// scope, cascade resolution tries parent scopes in order: project -> user -> org -> global.
func (r *SettingsRepo) Get(ctx context.Context, key string, scope string) (*model.Setting, error) {
	// Build the list of scopes to try, starting from the given scope and cascading up.
	scopes := cascadeScopes(scope)

	for _, s := range scopes {
		setting, err := r.getExact(ctx, key, s)
		if err == nil {
			return setting, nil
		}
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("settings get: %w", err)
		}
	}

	return nil, sql.ErrNoRows
}

// getExact retrieves a setting by exact key and scope, with no cascade.
func (r *SettingsRepo) getExact(ctx context.Context, key string, scope string) (*model.Setting, error) {
	query := `SELECT key, value, scope, updated_by, updated_at
		FROM settings WHERE key = ? AND scope = ?`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT key, value, scope, updated_by, updated_at
			FROM settings WHERE key = $1 AND scope = $2`
	}

	row := r.db.QueryRow(ctx, query, key, scope)
	return r.scanSetting(row)
}

// Set upserts a setting. If a setting with the same key and scope already exists,
// its value and updated_by are updated. UpdatedAt is always set to the current time.
func (r *SettingsRepo) Set(ctx context.Context, setting *model.Setting) error {
	now := time.Now().UTC().Format(time.RFC3339)

	var updatedBy *string
	if setting.UpdatedBy != nil {
		s := setting.UpdatedBy.String()
		updatedBy = &s
	}

	valueStr := string(setting.Value)

	query := `INSERT INTO settings (key, value, scope, updated_by, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (key, scope) DO UPDATE SET value = ?, updated_by = ?, updated_at = ?`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO settings (key, value, scope, updated_by, updated_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (key, scope) DO UPDATE SET value = $6, updated_by = $7, updated_at = $8`
	}

	_, err := r.db.Exec(ctx, query,
		setting.Key, valueStr, setting.Scope, updatedBy, now,
		valueStr, updatedBy, now,
	)
	if err != nil {
		return fmt.Errorf("settings set: %w", err)
	}

	return r.reload(ctx, setting)
}

// Delete removes a setting by its composite key (key, scope).
func (r *SettingsRepo) Delete(ctx context.Context, key string, scope string) error {
	query := `DELETE FROM settings WHERE key = ? AND scope = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM settings WHERE key = $1 AND scope = $2`
	}

	_, err := r.db.Exec(ctx, query, key, scope)
	if err != nil {
		return fmt.Errorf("settings delete: %w", err)
	}
	return nil
}

// ListByScope returns all settings for a given scope, ordered by key.
func (r *SettingsRepo) ListByScope(ctx context.Context, scope string) ([]model.Setting, error) {
	query := `SELECT key, value, scope, updated_by, updated_at
		FROM settings WHERE scope = ? ORDER BY key`
	if r.db.Backend() == BackendPostgres {
		query = `SELECT key, value, scope, updated_by, updated_at
			FROM settings WHERE scope = $1 ORDER BY key`
	}

	rows, err := r.db.Query(ctx, query, scope)
	if err != nil {
		return nil, fmt.Errorf("settings list by scope: %w", err)
	}
	defer rows.Close()

	var result []model.Setting
	for rows.Next() {
		setting, err := r.scanSettingFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *setting)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("settings list by scope iteration: %w", err)
	}
	return result, nil
}

// ListAll returns all settings ordered by key.
func (r *SettingsRepo) ListAll(ctx context.Context) ([]model.Setting, error) {
	query := `SELECT key, value, scope, updated_by, updated_at
		FROM settings ORDER BY key`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("settings list all: %w", err)
	}
	defer rows.Close()

	var result []model.Setting
	for rows.Next() {
		setting, err := r.scanSettingFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *setting)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("settings list all iteration: %w", err)
	}
	return result, nil
}

// GetSchema returns the schema/definition for a setting key from the "global" scope.
func (r *SettingsRepo) GetSchema(ctx context.Context, key string) (*model.Setting, error) {
	return r.getExact(ctx, key, "global")
}

// reload fetches the setting by key and scope and populates the struct in place.
func (r *SettingsRepo) reload(ctx context.Context, setting *model.Setting) error {
	fetched, err := r.getExact(ctx, setting.Key, setting.Scope)
	if err != nil {
		return fmt.Errorf("settings reload: %w", err)
	}
	*setting = *fetched
	return nil
}

// scanSetting scans a single row into a model.Setting.
func (r *SettingsRepo) scanSetting(row *sql.Row) (*model.Setting, error) {
	var setting model.Setting
	var valueStr string
	var updatedByStr *string
	var updatedAtStr string

	err := row.Scan(
		&setting.Key, &valueStr, &setting.Scope,
		&updatedByStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	setting.Value = json.RawMessage(valueStr)

	if updatedByStr != nil {
		uid, err := uuid.Parse(*updatedByStr)
		if err != nil {
			return nil, fmt.Errorf("settings scan parse updated_by: %w", err)
		}
		setting.UpdatedBy = &uid
	}

	setting.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("settings scan parse updated_at: %w", err)
	}

	return &setting, nil
}

// scanSettingFromRows scans the current row from sql.Rows into a model.Setting.
func (r *SettingsRepo) scanSettingFromRows(rows *sql.Rows) (*model.Setting, error) {
	var setting model.Setting
	var valueStr string
	var updatedByStr *string
	var updatedAtStr string

	err := rows.Scan(
		&setting.Key, &valueStr, &setting.Scope,
		&updatedByStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("settings scan rows: %w", err)
	}

	setting.Value = json.RawMessage(valueStr)

	if updatedByStr != nil {
		uid, err := uuid.Parse(*updatedByStr)
		if err != nil {
			return nil, fmt.Errorf("settings scan rows parse updated_by: %w", err)
		}
		setting.UpdatedBy = &uid
	}

	setting.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("settings scan rows parse updated_at: %w", err)
	}

	return &setting, nil
}

// cascadeScopes returns the list of scopes to try for cascade resolution,
// starting from the given scope and moving up to "global".
// Scope format: "global", "org:{id}", "user:{id}", "project:{id}".
func cascadeScopes(scope string) []string {
	// Find the position of the given scope's prefix in the scope order.
	prefix := scopePrefix(scope)
	startIdx := -1
	for i, p := range scopeOrder {
		if p == prefix {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		// Unknown scope; just try the given scope then global.
		if scope == "global" {
			return []string{"global"}
		}
		return []string{scope, "global"}
	}

	result := []string{scope}
	for i := startIdx + 1; i < len(scopeOrder); i++ {
		result = append(result, scopeOrder[i])
	}
	return result
}

// scopePrefix extracts the prefix before ":" from a scope string.
// For "global" it returns "global". For "org:abc" it returns "org".
func scopePrefix(scope string) string {
	for i, c := range scope {
		if c == ':' {
			return scope[:i]
		}
	}
	return scope
}
