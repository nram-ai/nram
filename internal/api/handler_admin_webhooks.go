package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// WebhookAdminStore abstracts storage operations for the webhook admin API.
type WebhookAdminStore interface {
	CountWebhooks(ctx context.Context) (int, error)
	ListWebhooks(ctx context.Context, limit, offset int) ([]model.Webhook, error)
	CreateWebhook(ctx context.Context, url, scope string, events []string, secret *string, active bool) (*model.Webhook, error)
	GetWebhook(ctx context.Context, id uuid.UUID) (*model.Webhook, error)
	UpdateWebhook(ctx context.Context, id uuid.UUID, url, scope string, events []string, secret *string, active bool) (*model.Webhook, error)
	DeleteWebhook(ctx context.Context, id uuid.UUID) error
	TestWebhook(ctx context.Context, id uuid.UUID) (*WebhookTestResult, error)
}

// WebhookAdminConfig holds the dependencies for the webhook admin handler.
type WebhookAdminConfig struct {
	Store WebhookAdminStore
}

// WebhookTestResult holds the outcome of a test webhook fire.
type WebhookTestResult struct {
	Success    bool   `json:"success"`
	StatusCode int    `json:"status_code"`
	Message    string `json:"message"`
	LatencyMs  int64  `json:"latency_ms"`
}

// webhookCreateRequest is the request body for POST /webhooks.
type webhookCreateRequest struct {
	URL    string   `json:"url"`
	Scope  string   `json:"scope"`
	Events []string `json:"events"`
	Secret *string  `json:"secret"`
	Active *bool    `json:"active"`
}

// webhookUpdateRequest is the request body for PUT /webhooks/{id}.
type webhookUpdateRequest struct {
	URL    string   `json:"url"`
	Scope  string   `json:"scope"`
	Events []string `json:"events"`
	Secret *string  `json:"secret"`
	Active *bool    `json:"active"`
}

// NewAdminWebhooksHandler returns an http.HandlerFunc that dispatches webhook
// admin requests based on method and sub-path under /webhooks.
//
// Routes:
//   - GET    /webhooks           — list all webhooks
//   - POST   /webhooks           — create a webhook
//   - GET    /webhooks/{id}      — get webhook detail
//   - PUT    /webhooks/{id}      — update a webhook
//   - DELETE /webhooks/{id}      — delete a webhook
//   - POST   /webhooks/{id}/test — fire a test event to a webhook
func NewAdminWebhooksHandler(cfg WebhookAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, action := extractWebhookSubPath(r.URL.Path)

		if id == "" && action == "" {
			// /webhooks root
			switch r.Method {
			case http.MethodGet:
				handleListWebhooks(w, r, cfg)
			case http.MethodPost:
				handleCreateWebhook(w, r, cfg)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		// Parse UUID
		parsed, err := uuid.Parse(id)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid webhook id"))
			return
		}

		if action == "test" {
			if r.Method != http.MethodPost {
				WriteError(w, ErrBadRequest("method not allowed"))
				return
			}
			handleTestWebhook(w, r, cfg, parsed)
			return
		}

		if action == "" {
			switch r.Method {
			case http.MethodGet:
				handleGetWebhook(w, r, cfg, parsed)
			case http.MethodPut:
				handleUpdateWebhook(w, r, cfg, parsed)
			case http.MethodDelete:
				handleDeleteWebhook(w, r, cfg, parsed)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		WriteError(w, ErrBadRequest("unknown webhook sub-path"))
	}
}

// extractWebhookSubPath parses the URL path after "/webhooks" into an ID segment
// and an optional action segment (e.g. "test").
func extractWebhookSubPath(path string) (id, action string) {
	const marker = "/webhooks"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return "", ""
	}
	rest := path[idx+len(marker):]
	rest = strings.TrimPrefix(rest, "/")
	if rest == "" {
		return "", ""
	}
	parts := strings.SplitN(rest, "/", 2)
	id = parts[0]
	if len(parts) > 1 {
		action = parts[1]
	}
	return id, action
}

// handleListWebhooks handles GET /webhooks.
func handleListWebhooks(w http.ResponseWriter, r *http.Request, cfg WebhookAdminConfig) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			limit = n
		}
	}
	if limit > 200 {
		limit = 200
	}

	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	total, err := cfg.Store.CountWebhooks(r.Context())
	if err != nil {
		WriteError(w, mapWebhookError(err))
		return
	}

	webhooks, err := cfg.Store.ListWebhooks(r.Context(), limit, offset)
	if err != nil {
		WriteError(w, mapWebhookError(err))
		return
	}
	if webhooks == nil {
		webhooks = []model.Webhook{}
	}

	writeJSON(w, http.StatusOK, model.PaginatedResponse[model.Webhook]{
		Data: webhooks,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	})
}

// handleCreateWebhook handles POST /webhooks.
func handleCreateWebhook(w http.ResponseWriter, r *http.Request, cfg WebhookAdminConfig) {
	var body webhookCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	body.URL = strings.TrimSpace(body.URL)
	if body.URL == "" {
		WriteError(w, ErrBadRequest("url is required"))
		return
	}

	if len(body.Events) == 0 {
		WriteError(w, ErrBadRequest("events must be a non-empty array"))
		return
	}

	if body.Scope == "" {
		body.Scope = "global"
	}

	active := true
	if body.Active != nil {
		active = *body.Active
	}

	webhook, err := cfg.Store.CreateWebhook(r.Context(), body.URL, body.Scope, body.Events, body.Secret, active)
	if err != nil {
		WriteError(w, mapWebhookError(err))
		return
	}

	writeJSON(w, http.StatusCreated, webhook)
}

// handleGetWebhook handles GET /webhooks/{id}.
func handleGetWebhook(w http.ResponseWriter, r *http.Request, cfg WebhookAdminConfig, id uuid.UUID) {
	webhook, err := cfg.Store.GetWebhook(r.Context(), id)
	if err != nil {
		WriteError(w, mapWebhookError(err))
		return
	}

	writeJSON(w, http.StatusOK, webhook)
}

// handleUpdateWebhook handles PUT /webhooks/{id}.
func handleUpdateWebhook(w http.ResponseWriter, r *http.Request, cfg WebhookAdminConfig, id uuid.UUID) {
	var body webhookUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	body.URL = strings.TrimSpace(body.URL)
	if body.URL == "" {
		WriteError(w, ErrBadRequest("url is required"))
		return
	}

	if len(body.Events) == 0 {
		WriteError(w, ErrBadRequest("events must be a non-empty array"))
		return
	}

	if body.Scope == "" {
		body.Scope = "global"
	}

	active := true
	if body.Active != nil {
		active = *body.Active
	}

	webhook, err := cfg.Store.UpdateWebhook(r.Context(), id, body.URL, body.Scope, body.Events, body.Secret, active)
	if err != nil {
		WriteError(w, mapWebhookError(err))
		return
	}

	writeJSON(w, http.StatusOK, webhook)
}

// handleDeleteWebhook handles DELETE /webhooks/{id}.
func handleDeleteWebhook(w http.ResponseWriter, r *http.Request, cfg WebhookAdminConfig, id uuid.UUID) {
	if err := cfg.Store.DeleteWebhook(r.Context(), id); err != nil {
		WriteError(w, mapWebhookError(err))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleTestWebhook handles POST /webhooks/{id}/test.
func handleTestWebhook(w http.ResponseWriter, r *http.Request, cfg WebhookAdminConfig, id uuid.UUID) {
	result, err := cfg.Store.TestWebhook(r.Context(), id)
	if err != nil {
		WriteError(w, mapWebhookError(err))
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// mapWebhookError maps store errors to appropriate API errors.
func mapWebhookError(err error) *APIError {
	msg := err.Error()
	if strings.Contains(msg, "not found") {
		return ErrNotFound(msg)
	}
	return ErrInternal(msg)
}
