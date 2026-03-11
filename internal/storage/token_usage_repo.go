package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// TokenUsageRepo provides append-only operations for the token_usage table.
type TokenUsageRepo struct {
	db DB
}

// NewTokenUsageRepo creates a new TokenUsageRepo backed by the given DB.
func NewTokenUsageRepo(db DB) *TokenUsageRepo {
	return &TokenUsageRepo{db: db}
}

// Record inserts a new token usage record (append-only). ID is generated if zero-valued.
func (r *TokenUsageRepo) Record(ctx context.Context, usage *model.TokenUsage) error {
	if usage.ID == uuid.Nil {
		usage.ID = uuid.New()
	}

	query := `INSERT INTO token_usage (id, org_id, user_id, project_id, namespace_id,
		operation, provider, model, tokens_input, tokens_output, memory_id, api_key_id, latency_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO token_usage (id, org_id, user_id, project_id, namespace_id,
			operation, provider, model, tokens_input, tokens_output, memory_id, api_key_id, latency_ms)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
	}

	_, err := r.db.Exec(ctx, query,
		usage.ID.String(),
		nullableUUIDStr(usage.OrgID),
		nullableUUIDStr(usage.UserID),
		nullableUUIDStr(usage.ProjectID),
		usage.NamespaceID.String(),
		usage.Operation,
		usage.Provider,
		usage.Model,
		usage.TokensInput,
		usage.TokensOutput,
		nullableUUIDStr(usage.MemoryID),
		nullableUUIDStr(usage.APIKeyID),
		usage.LatencyMs,
	)
	if err != nil {
		return fmt.Errorf("token usage record: %w", err)
	}

	return r.reload(ctx, usage)
}

// GetByID returns a token usage record by its UUID.
func (r *TokenUsageRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.TokenUsage, error) {
	query := selectTokenUsageColumns + ` FROM token_usage WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectTokenUsageColumns + ` FROM token_usage WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanTokenUsage(row)
}

// QueryByScope returns token usage records for a given scope within a time range,
// ordered by created_at DESC. The scope is matched as the operation field.
func (r *TokenUsageRepo) QueryByScope(ctx context.Context, scope string, from, to time.Time) ([]model.TokenUsage, error) {
	fromStr := from.UTC().Format(time.RFC3339)
	toStr := to.UTC().Format(time.RFC3339)

	query := selectTokenUsageColumns + ` FROM token_usage
		WHERE operation = ? AND created_at >= ? AND created_at <= ?
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectTokenUsageColumns + ` FROM token_usage
			WHERE operation = $1 AND created_at >= $2 AND created_at <= $3
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, scope, fromStr, toStr)
	if err != nil {
		return nil, fmt.Errorf("token usage query by scope: %w", err)
	}
	defer rows.Close()

	return r.scanTokenUsages(rows)
}

// Purge deletes all token usage records older than the given time and returns
// the number of deleted rows.
func (r *TokenUsageRepo) Purge(ctx context.Context, before time.Time) (int64, error) {
	beforeStr := before.UTC().Format(time.RFC3339)

	query := `DELETE FROM token_usage WHERE created_at < ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM token_usage WHERE created_at < $1`
	}

	result, err := r.db.Exec(ctx, query, beforeStr)
	if err != nil {
		return 0, fmt.Errorf("token usage purge: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("token usage purge rows affected: %w", err)
	}
	return count, nil
}

// reload fetches the token usage record by ID and populates the struct in place.
func (r *TokenUsageRepo) reload(ctx context.Context, usage *model.TokenUsage) error {
	fetched, err := r.GetByID(ctx, usage.ID)
	if err != nil {
		return fmt.Errorf("token usage reload: %w", err)
	}
	*usage = *fetched
	return nil
}

// nullableUUIDStr converts a *uuid.UUID to a *string suitable for SQL parameters.
func nullableUUIDStr(id *uuid.UUID) *string {
	if id == nil {
		return nil
	}
	s := id.String()
	return &s
}

const selectTokenUsageColumns = `SELECT id, org_id, user_id, project_id, namespace_id,
	operation, provider, model, tokens_input, tokens_output, memory_id, api_key_id,
	latency_ms, created_at`

func (r *TokenUsageRepo) scanTokenUsage(row *sql.Row) (*model.TokenUsage, error) {
	var usage model.TokenUsage
	var idStr string
	var orgIDStr, userIDStr, projectIDStr sql.NullString
	var namespaceIDStr string
	var memoryIDStr, apiKeyIDStr sql.NullString
	var latencyMs sql.NullInt64
	var createdAtStr string

	err := row.Scan(
		&idStr, &orgIDStr, &userIDStr, &projectIDStr, &namespaceIDStr,
		&usage.Operation, &usage.Provider, &usage.Model,
		&usage.TokensInput, &usage.TokensOutput,
		&memoryIDStr, &apiKeyIDStr, &latencyMs, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return populateTokenUsage(&usage, idStr, orgIDStr, userIDStr, projectIDStr,
		namespaceIDStr, memoryIDStr, apiKeyIDStr, latencyMs, createdAtStr)
}

func (r *TokenUsageRepo) scanTokenUsageFromRows(rows *sql.Rows) (*model.TokenUsage, error) {
	var usage model.TokenUsage
	var idStr string
	var orgIDStr, userIDStr, projectIDStr sql.NullString
	var namespaceIDStr string
	var memoryIDStr, apiKeyIDStr sql.NullString
	var latencyMs sql.NullInt64
	var createdAtStr string

	err := rows.Scan(
		&idStr, &orgIDStr, &userIDStr, &projectIDStr, &namespaceIDStr,
		&usage.Operation, &usage.Provider, &usage.Model,
		&usage.TokensInput, &usage.TokensOutput,
		&memoryIDStr, &apiKeyIDStr, &latencyMs, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("token usage scan rows: %w", err)
	}

	return populateTokenUsage(&usage, idStr, orgIDStr, userIDStr, projectIDStr,
		namespaceIDStr, memoryIDStr, apiKeyIDStr, latencyMs, createdAtStr)
}

func (r *TokenUsageRepo) scanTokenUsages(rows *sql.Rows) ([]model.TokenUsage, error) {
	var result []model.TokenUsage
	for rows.Next() {
		usage, err := r.scanTokenUsageFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *usage)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("token usage scan iteration: %w", err)
	}
	return result, nil
}

func populateTokenUsage(
	usage *model.TokenUsage,
	idStr string,
	orgIDStr, userIDStr, projectIDStr sql.NullString,
	namespaceIDStr string,
	memoryIDStr, apiKeyIDStr sql.NullString,
	latencyMs sql.NullInt64,
	createdAtStr string,
) (*model.TokenUsage, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("token usage parse id: %w", err)
	}
	usage.ID = id

	nsID, err := uuid.Parse(namespaceIDStr)
	if err != nil {
		return nil, fmt.Errorf("token usage parse namespace_id: %w", err)
	}
	usage.NamespaceID = nsID

	if orgIDStr.Valid {
		parsed, err := uuid.Parse(orgIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("token usage parse org_id: %w", err)
		}
		usage.OrgID = &parsed
	}

	if userIDStr.Valid {
		parsed, err := uuid.Parse(userIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("token usage parse user_id: %w", err)
		}
		usage.UserID = &parsed
	}

	if projectIDStr.Valid {
		parsed, err := uuid.Parse(projectIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("token usage parse project_id: %w", err)
		}
		usage.ProjectID = &parsed
	}

	if memoryIDStr.Valid {
		parsed, err := uuid.Parse(memoryIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("token usage parse memory_id: %w", err)
		}
		usage.MemoryID = &parsed
	}

	if apiKeyIDStr.Valid {
		parsed, err := uuid.Parse(apiKeyIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("token usage parse api_key_id: %w", err)
		}
		usage.APIKeyID = &parsed
	}

	if latencyMs.Valid {
		v := int(latencyMs.Int64)
		usage.LatencyMs = &v
	}

	usage.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("token usage parse created_at: %w", err)
	}

	return usage, nil
}
