package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/storage"
)

// maxAuditQueryLimit is a defensive ceiling against pathological caller
// values (e.g. limit=MaxInt). Legitimate paginated callers should still
// apply their own, tighter cap before reaching the store.
const maxAuditQueryLimit = 10000

// defaultAuditQueryLimit is applied when callers pass limit<=0.
const defaultAuditQueryLimit = 100

// clampAuditQueryLimit applies the floor and ceiling that bound any LIMIT
// the store will pass through to SQL. Pulled out for direct unit testing.
func clampAuditQueryLimit(limit int) int {
	if limit <= 0 {
		return defaultAuditQueryLimit
	}
	if limit > maxAuditQueryLimit {
		return maxAuditQueryLimit
	}
	return limit
}

// AuditStore implements api.AuditStore against the audit_events table.
type AuditStore struct {
	db storage.DB
}

// NewAuditStore creates a new AuditStore.
func NewAuditStore(db storage.DB) *AuditStore {
	return &AuditStore{db: db}
}

func (s *AuditStore) Append(ctx context.Context, event api.AuditEvent) error {
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}

	details := nullableJSON(event.Details)
	occurredAt := event.OccurredAt.UTC().Format("2006-01-02T15:04:05.000Z")

	if s.db.Backend() == storage.BackendPostgres {
		const q = `INSERT INTO audit_events
			(id, occurred_at, actor_user_id, actor_role, action, target_type, target_id, target_org_id, source_ip, user_agent, details)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
		_, err := s.db.Exec(ctx, q,
			event.ID.String(),
			occurredAt,
			nullableUUID(event.ActorUserID),
			nullableString(event.ActorRole),
			event.Action,
			nullableString(event.TargetType),
			nullableUUID(event.TargetID),
			nullableUUID(event.TargetOrgID),
			nullableString(event.SourceIP),
			nullableString(event.UserAgent),
			details,
		)
		if err != nil {
			return fmt.Errorf("audit append: %w", err)
		}
		return nil
	}

	const q = `INSERT INTO audit_events
		(id, occurred_at, actor_user_id, actor_role, action, target_type, target_id, target_org_id, source_ip, user_agent, details)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(ctx, q,
		event.ID.String(),
		occurredAt,
		nullableUUID(event.ActorUserID),
		nullableString(event.ActorRole),
		event.Action,
		nullableString(event.TargetType),
		nullableUUID(event.TargetID),
		nullableUUID(event.TargetOrgID),
		nullableString(event.SourceIP),
		nullableString(event.UserAgent),
		details,
	)
	if err != nil {
		return fmt.Errorf("audit append: %w", err)
	}
	return nil
}

func (s *AuditStore) Query(ctx context.Context, scope api.AuditScope, since time.Time, limit int) ([]api.AuditEvent, error) {
	limit = clampAuditQueryLimit(limit)

	conds := []string{}
	args := []any{}
	argIdx := 1
	ph := func() string {
		if s.db.Backend() == storage.BackendPostgres {
			p := fmt.Sprintf("$%d", argIdx)
			argIdx++
			return p
		}
		return "?"
	}

	if scope.ActorUserID != nil {
		conds = append(conds, "actor_user_id = "+ph())
		args = append(args, scope.ActorUserID.String())
	}
	if scope.TargetOrgID != nil {
		conds = append(conds, "target_org_id = "+ph())
		args = append(args, scope.TargetOrgID.String())
	}
	if !since.IsZero() {
		conds = append(conds, "occurred_at >= "+ph())
		args = append(args, since.UTC().Format("2006-01-02T15:04:05.000Z"))
	}

	where := ""
	if len(conds) > 0 {
		where = " WHERE " + strings.Join(conds, " AND ")
	}

	// LIMIT placeholder: postgres takes $N, sqlite takes ?. The argIdx state
	// persists from the WHERE-clause builder so postgres assigns the next $N.
	limitPh := ph()
	args = append(args, limit)

	q := `SELECT id, occurred_at, actor_user_id, actor_role, action, target_type,
			target_id, target_org_id, source_ip, user_agent, details
		FROM audit_events` + where + ` ORDER BY occurred_at DESC LIMIT ` + limitPh

	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("audit query: %w", err)
	}
	defer rows.Close()

	events := []api.AuditEvent{}
	for rows.Next() {
		var (
			idStr          string
			occurredAtStr  string
			actorUserIDStr sql.NullString
			actorRole      sql.NullString
			action         string
			targetType     sql.NullString
			targetIDStr    sql.NullString
			targetOrgIDStr sql.NullString
			sourceIP       sql.NullString
			userAgent      sql.NullString
			detailsRaw     sql.NullString
		)
		if err := rows.Scan(&idStr, &occurredAtStr, &actorUserIDStr, &actorRole,
			&action, &targetType, &targetIDStr, &targetOrgIDStr,
			&sourceIP, &userAgent, &detailsRaw); err != nil {
			return nil, fmt.Errorf("audit scan: %w", err)
		}

		ev := api.AuditEvent{
			Action: action,
		}
		if id, err := uuid.Parse(idStr); err == nil {
			ev.ID = id
		}
		ev.OccurredAt = parseAuditTime(occurredAtStr)
		ev.ActorUserID = parseNullableUUID(actorUserIDStr)
		if actorRole.Valid {
			ev.ActorRole = actorRole.String
		}
		if targetType.Valid {
			ev.TargetType = targetType.String
		}
		ev.TargetID = parseNullableUUID(targetIDStr)
		ev.TargetOrgID = parseNullableUUID(targetOrgIDStr)
		if sourceIP.Valid {
			ev.SourceIP = sourceIP.String
		}
		if userAgent.Valid {
			ev.UserAgent = userAgent.String
		}
		if detailsRaw.Valid && detailsRaw.String != "" {
			ev.Details = json.RawMessage(detailsRaw.String)
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("audit iteration: %w", err)
	}
	return events, nil
}

func nullableUUID(id *uuid.UUID) any {
	if id == nil {
		return nil
	}
	return id.String()
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableJSON(raw json.RawMessage) any {
	if len(raw) == 0 {
		return "{}"
	}
	return string(raw)
}

func parseAuditTime(s string) time.Time {
	for _, layout := range []string{
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		time.RFC3339Nano,
		time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
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
