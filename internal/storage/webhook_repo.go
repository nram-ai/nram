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

// WebhookRepo provides CRUD operations for the webhooks table.
type WebhookRepo struct {
	db DB
}

// NewWebhookRepo creates a new WebhookRepo backed by the given DB.
func NewWebhookRepo(db DB) *WebhookRepo {
	return &WebhookRepo{db: db}
}

// encodeEvents serializes events for storage.
// SQLite: JSON array string. Postgres: TEXT[] literal {a,b,c}.
func (r *WebhookRepo) encodeEvents(events []string) (string, error) {
	if r.db.Backend() == BackendPostgres {
		return "{" + strings.Join(events, ",") + "}", nil
	}
	b, err := json.Marshal(events)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeEvents deserializes events from storage.
func (r *WebhookRepo) decodeEvents(s string) ([]string, error) {
	if r.db.Backend() == BackendPostgres {
		// Postgres TEXT[] comes back as {a,b,c}
		s = strings.TrimPrefix(s, "{")
		s = strings.TrimSuffix(s, "}")
		if s == "" {
			return []string{}, nil
		}
		return strings.Split(s, ","), nil
	}
	var events []string
	if err := json.Unmarshal([]byte(s), &events); err != nil {
		return nil, err
	}
	return events, nil
}

// encodeActive returns the appropriate value for the active column.
func (r *WebhookRepo) encodeActive(active bool) interface{} {
	if r.db.Backend() == BackendPostgres {
		return active
	}
	if active {
		return 1
	}
	return 0
}

// Create inserts a new webhook. ID is generated if zero-valued.
// Events defaults to `[]` if nil.
func (r *WebhookRepo) Create(ctx context.Context, webhook *model.Webhook) error {
	if webhook.ID == uuid.Nil {
		webhook.ID = uuid.New()
	}
	if webhook.Events == nil {
		webhook.Events = []string{}
	}

	eventsVal, err := r.encodeEvents(webhook.Events)
	if err != nil {
		return fmt.Errorf("webhook create marshal events: %w", err)
	}

	query := `INSERT INTO webhooks (id, url, secret, events, scope, active, failure_count)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	if r.db.Backend() == BackendPostgres {
		query = `INSERT INTO webhooks (id, url, secret, events, scope, active, failure_count)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`
	}

	_, err = r.db.Exec(ctx, query,
		webhook.ID.String(), webhook.URL, webhook.Secret,
		eventsVal, webhook.Scope, r.encodeActive(webhook.Active), webhook.FailureCount,
	)
	if err != nil {
		return fmt.Errorf("webhook create: %w", err)
	}

	return r.reload(ctx, webhook)
}

// GetByID returns a webhook by its UUID.
func (r *WebhookRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Webhook, error) {
	query := selectWebhookColumns + ` FROM webhooks WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = selectWebhookColumns + ` FROM webhooks WHERE id = $1`
	}

	row := r.db.QueryRow(ctx, query, id.String())
	return r.scanWebhook(row)
}

// Update updates mutable fields of a webhook.
func (r *WebhookRepo) Update(ctx context.Context, webhook *model.Webhook) error {
	if webhook.Events == nil {
		webhook.Events = []string{}
	}

	eventsVal, err := r.encodeEvents(webhook.Events)
	if err != nil {
		return fmt.Errorf("webhook update marshal events: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE webhooks SET url = ?, secret = ?, events = ?, scope = ?, active = ?, updated_at = ?
		WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE webhooks SET url = $1, secret = $2, events = $3, scope = $4, active = $5, updated_at = $6
			WHERE id = $7`
	}

	_, err = r.db.Exec(ctx, query,
		webhook.URL, webhook.Secret, eventsVal, webhook.Scope, r.encodeActive(webhook.Active), now,
		webhook.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("webhook update: %w", err)
	}

	return r.reload(ctx, webhook)
}

// Delete removes a webhook by its UUID (hard delete).
func (r *WebhookRepo) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM webhooks WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `DELETE FROM webhooks WHERE id = $1`
	}

	_, err := r.db.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("webhook delete: %w", err)
	}
	return nil
}

// ListActiveForEvent returns webhooks that are active and subscribed to the
// given event within a namespace scope. The scope is matched as "ns:{namespaceID}".
func (r *WebhookRepo) ListActiveForEvent(ctx context.Context, namespaceID uuid.UUID, event string) ([]model.Webhook, error) {
	scope := "ns:" + namespaceID.String()

	// SQLite: events is TEXT (JSON array), use LIKE for membership.
	// Postgres: events is TEXT[], use ANY() for membership.
	query := selectWebhookColumns + ` FROM webhooks
		WHERE active = 1 AND scope = ? AND events LIKE ?
		ORDER BY created_at DESC`
	var args []interface{}
	if r.db.Backend() == BackendPostgres {
		query = selectWebhookColumns + ` FROM webhooks
			WHERE active = true AND scope = $1 AND $2 = ANY(events)
			ORDER BY created_at DESC`
		args = []interface{}{scope, event}
	} else {
		pattern := "%" + `"` + event + `"` + "%"
		args = []interface{}{scope, pattern}
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("webhook list active for event: %w", err)
	}
	defer rows.Close()

	return r.scanWebhooks(rows)
}

// RecordFailure increments consecutive failure count. If failure_count reaches
// 10, the webhook is auto-disabled.
func (r *WebhookRepo) RecordFailure(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE webhooks SET
		failure_count = failure_count + 1,
		active = CASE WHEN failure_count + 1 >= 10 THEN 0 ELSE active END,
		updated_at = ?
		WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE webhooks SET
			failure_count = failure_count + 1,
			active = CASE WHEN failure_count + 1 >= 10 THEN false ELSE active END,
			updated_at = $1
			WHERE id = $2`
	}

	_, err := r.db.Exec(ctx, query, now, id.String())
	if err != nil {
		return fmt.Errorf("webhook record failure: %w", err)
	}
	return nil
}

// RecordSuccess resets failure_count to 0 and updates last_fired.
func (r *WebhookRepo) RecordSuccess(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC().Format(time.RFC3339)

	query := `UPDATE webhooks SET failure_count = 0, last_fired = ?, updated_at = ?
		WHERE id = ?`
	if r.db.Backend() == BackendPostgres {
		query = `UPDATE webhooks SET failure_count = 0, last_fired = $1, updated_at = $2
			WHERE id = $3`
	}

	_, err := r.db.Exec(ctx, query, now, now, id.String())
	if err != nil {
		return fmt.Errorf("webhook record success: %w", err)
	}
	return nil
}

// ListByNamespace returns all webhooks for a namespace scope, ordered by created_at DESC.
func (r *WebhookRepo) ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Webhook, error) {
	scope := "ns:" + namespaceID.String()

	query := selectWebhookColumns + ` FROM webhooks WHERE scope = ?
		ORDER BY created_at DESC`
	if r.db.Backend() == BackendPostgres {
		query = selectWebhookColumns + ` FROM webhooks WHERE scope = $1
			ORDER BY created_at DESC`
	}

	rows, err := r.db.Query(ctx, query, scope)
	if err != nil {
		return nil, fmt.Errorf("webhook list by namespace: %w", err)
	}
	defer rows.Close()

	return r.scanWebhooks(rows)
}

// ListAll returns all webhooks ordered by created_at DESC.
func (r *WebhookRepo) ListAll(ctx context.Context) ([]model.Webhook, error) {
	query := selectWebhookColumns + ` FROM webhooks ORDER BY created_at DESC`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("webhook list all: %w", err)
	}
	defer rows.Close()

	return r.scanWebhooks(rows)
}

// reload fetches the webhook by ID and populates the struct in place.
func (r *WebhookRepo) reload(ctx context.Context, webhook *model.Webhook) error {
	fetched, err := r.GetByID(ctx, webhook.ID)
	if err != nil {
		return fmt.Errorf("webhook reload: %w", err)
	}
	*webhook = *fetched
	return nil
}

const selectWebhookColumns = `SELECT id, url, secret, events, scope, active,
	last_fired, last_status, failure_count, created_at, updated_at`

func (r *WebhookRepo) scanWebhook(row *sql.Row) (*model.Webhook, error) {
	var webhook model.Webhook
	var idStr string
	var eventsStr string
	var active bool
	var lastFiredStr sql.NullString
	var lastStatus sql.NullInt64
	var createdAtStr, updatedAtStr string

	err := row.Scan(
		&idStr, &webhook.URL, &webhook.Secret, &eventsStr, &webhook.Scope, &active,
		&lastFiredStr, &lastStatus, &webhook.FailureCount, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	return r.populateWebhook(&webhook, idStr, eventsStr, active, lastFiredStr, lastStatus, createdAtStr, updatedAtStr)
}

func (r *WebhookRepo) scanWebhookFromRows(rows *sql.Rows) (*model.Webhook, error) {
	var webhook model.Webhook
	var idStr string
	var eventsStr string
	var active bool
	var lastFiredStr sql.NullString
	var lastStatus sql.NullInt64
	var createdAtStr, updatedAtStr string

	err := rows.Scan(
		&idStr, &webhook.URL, &webhook.Secret, &eventsStr, &webhook.Scope, &active,
		&lastFiredStr, &lastStatus, &webhook.FailureCount, &createdAtStr, &updatedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("webhook scan rows: %w", err)
	}

	return r.populateWebhook(&webhook, idStr, eventsStr, active, lastFiredStr, lastStatus, createdAtStr, updatedAtStr)
}

func (r *WebhookRepo) scanWebhooks(rows *sql.Rows) ([]model.Webhook, error) {
	var result []model.Webhook
	for rows.Next() {
		webhook, err := r.scanWebhookFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *webhook)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("webhook scan iteration: %w", err)
	}
	return result, nil
}

func (r *WebhookRepo) populateWebhook(
	webhook *model.Webhook,
	idStr, eventsStr string,
	active bool,
	lastFiredStr sql.NullString,
	lastStatus sql.NullInt64,
	createdAtStr, updatedAtStr string,
) (*model.Webhook, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("webhook parse id: %w", err)
	}
	webhook.ID = id

	events, err := r.decodeEvents(eventsStr)
	if err != nil {
		return nil, fmt.Errorf("webhook parse events: %w", err)
	}
	webhook.Events = events

	webhook.Active = active

	if lastFiredStr.Valid {
		t, err := time.Parse(time.RFC3339, lastFiredStr.String)
		if err != nil {
			return nil, fmt.Errorf("webhook parse last_fired: %w", err)
		}
		webhook.LastFired = &t
	}

	if lastStatus.Valid {
		status := int(lastStatus.Int64)
		webhook.LastStatus = &status
	}

	webhook.CreatedAt, err = time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("webhook parse created_at: %w", err)
	}
	webhook.UpdatedAt, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("webhook parse updated_at: %w", err)
	}

	return webhook, nil
}
