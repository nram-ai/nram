package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
)

// LogEntryRepo provides operations for the log_entries table: the rolling
// diagnostic-log store. It mirrors the dream_log_repo conventions (backend-
// switched placeholders, UUID-as-string, RFC3339 timestamps) so a single table
// definition serves both Postgres and SQLite.
type LogEntryRepo struct {
	db DB
}

// NewLogEntryRepo creates a new LogEntryRepo backed by the given DB.
func NewLogEntryRepo(db DB) *LogEntryRepo {
	return &LogEntryRepo{db: db}
}

// LogFilter narrows a List/Count query. All fields are optional; the zero value
// matches every row. Levels is an OR-set (level IN (...)); Search is a
// case-insensitive substring match on the message; AttrKey/AttrValue match a
// single structured field inside the attrs JSON object.
type LogFilter struct {
	Levels    []string
	Component string
	Search    string
	AttrKey   string
	AttrValue string
	From      *time.Time
	To        *time.Time
}

const logEntryColumns = `id, ts, level, component, message, attrs, project_id, namespace_id, user_id`

const logEntryInsertCols = 9

// BatchCreate inserts many log entries in chunked multi-row INSERTs, matching
// the IngestionLogRepo.BatchCreate pattern. Each row carries a pre-assigned id
// and an explicit RFC3339 timestamp; defaults are applied per row.
func (r *LogEntryRepo) BatchCreate(ctx context.Context, entries []*model.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	isPg := r.db.Backend() == BackendPostgres
	cols := "(" + logEntryColumns + ")"
	for start := 0; start < len(entries); start += batchInsertChunkSize {
		end := min(start+batchInsertChunkSize, len(entries))
		chunk := entries[start:end]
		args := make([]any, 0, logEntryInsertCols*len(chunk))
		for _, e := range chunk {
			if e.ID == uuid.Nil {
				e.ID = uuid.New()
			}
			if e.Timestamp.IsZero() {
				e.Timestamp = time.Now().UTC()
			}
			if len(e.Attrs) == 0 {
				e.Attrs = json.RawMessage("{}")
			}
			args = append(args,
				e.ID.String(),
				e.Timestamp.UTC().Format(time.RFC3339Nano),
				e.Level,
				nullableString(e.Component),
				e.Message,
				string(e.Attrs),
				nullableUUIDStr(e.ProjectID),
				nullableUUIDStr(e.NamespaceID),
				nullableUUIDStr(e.UserID),
			)
		}
		query := "INSERT INTO log_entries " + cols + " VALUES " + multiRowPlaceholders(isPg, len(chunk), logEntryInsertCols)
		if _, err := r.db.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("log_entries batch create: %w", err)
		}
	}
	return nil
}

// List returns log entries matching the filter, newest first, with pagination.
func (r *LogEntryRepo) List(ctx context.Context, f LogFilter, limit, offset int) ([]model.LogEntry, error) {
	wb := &whereBuilder{postgres: r.db.Backend() == BackendPostgres}
	r.applyFilter(wb, f)

	limitPH := wb.placeholder()
	wb.args = append(wb.args, limit)
	offsetPH := wb.placeholder()
	wb.args = append(wb.args, offset)

	query := `SELECT ` + logEntryColumns + ` FROM log_entries` + whereClause(wb) +
		` ORDER BY ts DESC LIMIT ` + limitPH + ` OFFSET ` + offsetPH

	rows, err := r.db.Query(ctx, query, wb.args...)
	if err != nil {
		return nil, fmt.Errorf("log_entries list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanLogEntries(rows)
}

// Count returns the number of log entries matching the filter.
func (r *LogEntryRepo) Count(ctx context.Context, f LogFilter) (int, error) {
	wb := &whereBuilder{postgres: r.db.Backend() == BackendPostgres}
	r.applyFilter(wb, f)
	query := `SELECT COUNT(*) FROM log_entries` + whereClause(wb)

	var count int
	if err := r.db.QueryRow(ctx, query, wb.args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("log_entries count: %w", err)
	}
	return count, nil
}

// DeleteOlderThan removes log entries whose timestamp predates the cutoff.
// Returns the number of rows deleted. Used by the age-based retention limit.
func (r *LogEntryRepo) DeleteOlderThan(ctx context.Context, before time.Time) (int64, error) {
	beforeStr := before.UTC().Format(time.RFC3339Nano)
	query := `DELETE FROM log_entries WHERE ts < ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM log_entries WHERE ts < $1`
	}
	result, err := r.db.Exec(ctx, query, beforeStr)
	if err != nil {
		return 0, fmt.Errorf("log_entries delete older than: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("log_entries delete older than rows affected: %w", err)
	}
	return n, nil
}

// DeleteBeyondCount removes the oldest rows so that at most keep newest rows
// remain. keep <= 0 is a no-op (the count limit is disabled). Returns the
// number of rows deleted. Used by the count-based rolling-window limit.
func (r *LogEntryRepo) DeleteBeyondCount(ctx context.Context, keep int) (int64, error) {
	if keep <= 0 {
		return 0, nil
	}
	// SQLite requires a LIMIT before OFFSET; LIMIT -1 means "no limit". Postgres
	// accepts a bare OFFSET. Both delete the ids ranked past the newest `keep`.
	query := `DELETE FROM log_entries WHERE id IN (
		SELECT id FROM log_entries ORDER BY ts DESC LIMIT -1 OFFSET ?)`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM log_entries WHERE id IN (
			SELECT id FROM log_entries ORDER BY ts DESC OFFSET $1)`
	}
	result, err := r.db.Exec(ctx, query, keep)
	if err != nil {
		return 0, fmt.Errorf("log_entries delete beyond count: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("log_entries delete beyond count rows affected: %w", err)
	}
	return n, nil
}

// Components returns the distinct, non-empty component values present in the
// table, ordered alphabetically, for populating the Logs page filter dropdown.
func (r *LogEntryRepo) Components(ctx context.Context) ([]string, error) {
	rows, err := r.db.Query(ctx, `SELECT DISTINCT component FROM log_entries WHERE component IS NOT NULL AND component <> '' ORDER BY component`)
	if err != nil {
		return nil, fmt.Errorf("log_entries components: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, fmt.Errorf("log_entries components scan: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("log_entries components iteration: %w", err)
	}
	return out, nil
}

// applyFilter appends the filter predicates to the shared whereBuilder.
func (r *LogEntryRepo) applyFilter(wb *whereBuilder, f LogFilter) {
	if len(f.Levels) > 0 {
		placeholders := make([]string, len(f.Levels))
		for i, lvl := range f.Levels {
			placeholders[i] = wb.placeholder()
			wb.args = append(wb.args, lvl)
		}
		wb.clauses = append(wb.clauses, "level IN ("+strings.Join(placeholders, ", ")+")")
	}
	if f.Component != "" {
		wb.add("component = %s", f.Component)
	}
	if f.Search != "" {
		wb.add("LOWER(message) LIKE %s", "%"+strings.ToLower(f.Search)+"%")
	}
	if f.AttrKey != "" {
		keyPH := wb.placeholder()
		if wb.postgres {
			wb.args = append(wb.args, f.AttrKey)
		} else {
			wb.args = append(wb.args, "$."+f.AttrKey)
		}
		valPH := wb.placeholder()
		wb.args = append(wb.args, f.AttrValue)
		if wb.postgres {
			wb.clauses = append(wb.clauses, "attrs ->> "+keyPH+" = "+valPH)
		} else {
			wb.clauses = append(wb.clauses, "json_extract(attrs, "+keyPH+") = "+valPH)
		}
	}
	if f.From != nil {
		wb.add("ts >= %s", f.From.UTC().Format(time.RFC3339Nano))
	}
	if f.To != nil {
		wb.add("ts <= %s", f.To.UTC().Format(time.RFC3339Nano))
	}
}

// whereClause renders the builder's clauses as a leading " WHERE ..." fragment,
// or the empty string when no predicates were added.
func whereClause(wb *whereBuilder) string {
	if len(wb.clauses) == 0 {
		return ""
	}
	return " WHERE " + wb.where()
}

// nullableString returns nil for an empty string so the column stores NULL.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func scanLogEntries(rows *sql.Rows) ([]model.LogEntry, error) {
	var result []model.LogEntry
	for rows.Next() {
		var e model.LogEntry
		var idStr, attrsStr, tsStr string
		var component sql.NullString
		var projectID, namespaceID, userID sql.NullString

		if err := rows.Scan(
			&idStr, &tsStr, &e.Level, &component, &e.Message, &attrsStr,
			&projectID, &namespaceID, &userID,
		); err != nil {
			return nil, fmt.Errorf("log_entries scan: %w", err)
		}

		e.ID, _ = uuid.Parse(idStr)
		e.Timestamp, _ = parseRFC3339(tsStr)
		e.Component = component.String
		e.Attrs = json.RawMessage(attrsStr)
		e.ProjectID = parseNullableUUID(projectID)
		e.NamespaceID = parseNullableUUID(namespaceID)
		e.UserID = parseNullableUUID(userID)

		result = append(result, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("log_entries scan iteration: %w", err)
	}
	return result, nil
}

func parseNullableUUID(ns sql.NullString) *uuid.UUID {
	if !ns.Valid || ns.String == "" {
		return nil
	}
	id, err := uuid.Parse(ns.String)
	if err != nil {
		return nil
	}
	return &id
}
