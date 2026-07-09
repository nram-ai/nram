package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
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
		operation, provider, model, tokens_input, tokens_output, memory_id, api_key_id,
		latency_ms, success, error_code, request_id, cycle_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO token_usage (id, org_id, user_id, project_id, namespace_id,
			operation, provider, model, tokens_input, tokens_output, memory_id, api_key_id,
			latency_ms, success, error_code, request_id, cycle_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`
	}

	// memoryID is the only argument that varies across the FK-tolerant retry
	// below, so it is the single parameter to buildArgs; every other column is
	// fixed for this row.
	buildArgs := func(memoryID *string) []any {
		return []any{
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
			memoryID,
			nullableUUIDStr(usage.APIKeyID),
			usage.LatencyMs,
			EncodeBool(r.db.Backend(), usage.Success),
			usage.ErrorCode,
			usage.RequestID,
			nullableUUIDStr(usage.CycleID),
		}
	}

	_, err := r.db.Exec(ctx, query, buildArgs(nullableUUIDStr(usage.MemoryID))...)
	if err != nil && usage.MemoryID != nil && isForeignKeyViolation(err) {
		// The memory referenced by this row does not exist. The expected cause
		// is a delete/forget race (a memory hard-deleted between the upstream
		// provider call and this best-effort accounting write). Keep the row
		// for billing/analytics; drop only the now-dangling memory link, which
		// the schema would itself null on delete (ON DELETE SET NULL). Retry
		// exactly once with the link cleared.
		//
		// This also fires — and is the ONLY signal for — a caller that records
		// usage against a memory it has not persisted yet (a write-ordering
		// bug). Log loudly so that case surfaces instead of silently nulling.
		reqID := ""
		if usage.RequestID != nil {
			reqID = *usage.RequestID
		}
		slog.Warn("token_usage: memory_id foreign key violation; recording row with null memory link (memory absent: deleted mid-flight, or usage recorded before its memory row was persisted)",
			"memory_id", usage.MemoryID.String(),
			"operation", usage.Operation,
			"provider", usage.Provider,
			"request_id", reqID)
		usage.MemoryID = nil
		_, err = r.db.Exec(ctx, query, buildArgs(nil)...)
	}
	if err != nil {
		return fmt.Errorf("token usage record: %w", err)
	}

	return r.reload(ctx, usage)
}

// ReassignProjectTx updates all token_usage records from one project to another
// inside the caller's transaction. Updates namespace_id alongside project_id
// so we don't leave rows dangling on the old namespace once it is deleted.
// Used by the project-delete cascade so the reassign and the project row
// delete commit together.
func (r *TokenUsageRepo) ReassignProjectTx(ctx context.Context, tx *sql.Tx, fromProjectID, toProjectID uuid.UUID, toNamespaceID uuid.UUID) error {
	query := `UPDATE token_usage SET project_id = ?, namespace_id = ? WHERE project_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE token_usage SET project_id = $1, namespace_id = $2 WHERE project_id = $3`
	}
	if _, err := tx.ExecContext(ctx, query, toProjectID.String(), toNamespaceID.String(), fromProjectID.String()); err != nil {
		return fmt.Errorf("token usage reassign project: %w", err)
	}
	return nil
}

// ReassignNamespaceTx updates all token_usage records still referencing a
// namespace (regardless of their project_id) to point at a different project
// and namespace. Catches rows that ReassignProjectTx misses: project_id NULL
// or pointing at a different project but namespace_id still on the namespace
// being deleted. Required because token_usage.namespace_id has no ON DELETE
// action and is NOT NULL, so the namespace row delete fails otherwise.
func (r *TokenUsageRepo) ReassignNamespaceTx(ctx context.Context, tx *sql.Tx, fromNamespaceID, toProjectID, toNamespaceID uuid.UUID) error {
	query := `UPDATE token_usage SET project_id = ?, namespace_id = ? WHERE namespace_id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE token_usage SET project_id = $1, namespace_id = $2 WHERE namespace_id = $3`
	}
	if _, err := tx.ExecContext(ctx, query, toProjectID.String(), toNamespaceID.String(), fromNamespaceID.String()); err != nil {
		return fmt.Errorf("token usage reassign namespace: %w", err)
	}
	return nil
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
	defer func() { _ = rows.Close() }()

	return r.scanTokenUsages(rows)
}

// ListByMemoryIDs returns token_usage rows for the given memories restricted to
// the provided operations, ordered by created_at DESC. Batched (single query
// with IN clauses) to avoid N+1 over a queue page; memoryIDs is expected to be
// page-sized. Returns nil when either filter list is empty.
func (r *TokenUsageRepo) ListByMemoryIDs(ctx context.Context, memoryIDs []uuid.UUID, operations []string) ([]model.TokenUsage, error) {
	if len(memoryIDs) == 0 || len(operations) == 0 {
		return nil, nil
	}

	// Reuse the shared UUID IN-list builder for the memory IDs; the operation
	// strings continue the Postgres placeholder numbering after them.
	memPlaceholders, memArgs := uuidInPlaceholders(r.db, memoryIDs, 1)

	pg := r.db.Backend() == BackendPostgres
	opPlaceholders := make([]string, len(operations))
	args := make([]any, 0, len(memArgs)+len(operations))
	args = append(args, memArgs...)
	for i, op := range operations {
		if pg {
			opPlaceholders[i] = fmt.Sprintf("$%d", len(memoryIDs)+1+i)
		} else {
			opPlaceholders[i] = "?"
		}
		args = append(args, op)
	}

	query := selectTokenUsageColumns + ` FROM token_usage
		WHERE memory_id IN (` + strings.Join(memPlaceholders, ", ") + `)
		AND operation IN (` + strings.Join(opPlaceholders, ", ") + `)
		ORDER BY created_at DESC`

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("token usage list by memory ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
	latency_ms, success, error_code, request_id, cycle_id, created_at`

func (r *TokenUsageRepo) scanTokenUsage(row *sql.Row) (*model.TokenUsage, error) {
	var usage model.TokenUsage
	var idStr string
	var orgIDStr, userIDStr, projectIDStr sql.NullString
	var namespaceIDStr string
	var memoryIDStr, apiKeyIDStr sql.NullString
	var latencyMs sql.NullInt64
	var success bool
	var errorCode, requestID, cycleIDStr sql.NullString
	var createdAtStr string

	err := row.Scan(
		&idStr, &orgIDStr, &userIDStr, &projectIDStr, &namespaceIDStr,
		&usage.Operation, &usage.Provider, &usage.Model,
		&usage.TokensInput, &usage.TokensOutput,
		&memoryIDStr, &apiKeyIDStr, &latencyMs,
		&success, &errorCode, &requestID, &cycleIDStr, &createdAtStr,
	)
	if err != nil {
		return nil, err
	}

	return populateTokenUsage(&usage, idStr, orgIDStr, userIDStr, projectIDStr,
		namespaceIDStr, memoryIDStr, apiKeyIDStr, latencyMs,
		success, errorCode, requestID, cycleIDStr, createdAtStr)
}

func (r *TokenUsageRepo) scanTokenUsageFromRows(rows *sql.Rows) (*model.TokenUsage, error) {
	var usage model.TokenUsage
	var idStr string
	var orgIDStr, userIDStr, projectIDStr sql.NullString
	var namespaceIDStr string
	var memoryIDStr, apiKeyIDStr sql.NullString
	var latencyMs sql.NullInt64
	var success bool
	var errorCode, requestID, cycleIDStr sql.NullString
	var createdAtStr string

	err := rows.Scan(
		&idStr, &orgIDStr, &userIDStr, &projectIDStr, &namespaceIDStr,
		&usage.Operation, &usage.Provider, &usage.Model,
		&usage.TokensInput, &usage.TokensOutput,
		&memoryIDStr, &apiKeyIDStr, &latencyMs,
		&success, &errorCode, &requestID, &cycleIDStr, &createdAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("token usage scan rows: %w", err)
	}

	return populateTokenUsage(&usage, idStr, orgIDStr, userIDStr, projectIDStr,
		namespaceIDStr, memoryIDStr, apiKeyIDStr, latencyMs,
		success, errorCode, requestID, cycleIDStr, createdAtStr)
}

func (r *TokenUsageRepo) scanTokenUsages(rows *sql.Rows) ([]model.TokenUsage, error) {
	result := []model.TokenUsage{}
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
	success bool,
	errorCode, requestID, cycleIDStr sql.NullString,
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

	usage.Success = success

	if errorCode.Valid {
		s := errorCode.String
		usage.ErrorCode = &s
	}
	if requestID.Valid {
		s := requestID.String
		usage.RequestID = &s
	}
	if cycleIDStr.Valid {
		parsed, err := uuid.Parse(cycleIDStr.String)
		if err != nil {
			return nil, fmt.Errorf("token usage parse cycle_id: %w", err)
		}
		usage.CycleID = &parsed
	}

	usage.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("token usage parse created_at: %w", err)
	}

	return usage, nil
}
