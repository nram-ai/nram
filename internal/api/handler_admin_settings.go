package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// SettingsAdminStore abstracts storage operations for the settings admin API.
type SettingsAdminStore interface {
	ListSettings(ctx context.Context, scope string) ([]model.Setting, error)
	UpdateSetting(ctx context.Context, key string, value json.RawMessage, scope string, updatedBy *uuid.UUID) error
	GetSettingsSchema(ctx context.Context) ([]SettingSchema, error)
}

// SettingsAdminConfig holds the dependencies for the settings admin handler.
type SettingsAdminConfig struct {
	Store SettingsAdminStore
}

// SettingSchema describes a single setting definition with its type and default.
type SettingSchema struct {
	Key          string          `json:"key"`
	Type         string          `json:"type"`
	DefaultValue json.RawMessage `json:"default_value"`
	Description  string          `json:"description"`
	Category     string          `json:"category"`
}

// settingUpdateRequest is the request body for PUT /settings.
type settingUpdateRequest struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
	Scope string          `json:"scope"`
}

// NewAdminSettingsHandler returns an http.HandlerFunc that dispatches settings
// admin requests based on method and query parameters.
//
// Routes:
//   - GET  /settings              — list settings (optional ?scope= filter)
//   - GET  /settings?schema=true  — return setting definitions with types/defaults
//   - PUT  /settings              — update a setting (key, value, scope in body)
func NewAdminSettingsHandler(cfg SettingsAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("schema") == "true" {
				handleSettingsSchema(w, r, cfg)
			} else {
				handleListSettings(w, r, cfg)
			}
		case http.MethodPut:
			handleUpdateSetting(w, r, cfg)
		default:
			WriteError(w, ErrBadRequest("method not allowed"))
		}
	}
}

// handleListSettings handles GET /settings — returns settings optionally filtered by scope.
func handleListSettings(w http.ResponseWriter, r *http.Request, cfg SettingsAdminConfig) {
	scope := r.URL.Query().Get("scope")

	settings, err := cfg.Store.ListSettings(r.Context(), scope)
	if err != nil {
		WriteError(w, mapSettingsError(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"data": settings})
}

// handleSettingsSchema handles GET /settings?schema=true — returns setting definitions.
func handleSettingsSchema(w http.ResponseWriter, r *http.Request, cfg SettingsAdminConfig) {
	schemas, err := cfg.Store.GetSettingsSchema(r.Context())
	if err != nil {
		WriteError(w, mapSettingsError(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"data": schemas})
}

// handleUpdateSetting handles PUT /settings — updates a setting by key.
func handleUpdateSetting(w http.ResponseWriter, r *http.Request, cfg SettingsAdminConfig) {
	var body settingUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	body.Key = strings.TrimSpace(body.Key)
	if body.Key == "" {
		WriteError(w, ErrBadRequest("key is required"))
		return
	}

	if len(body.Value) == 0 {
		WriteError(w, ErrBadRequest("value is required"))
		return
	}

	if body.Scope == "" {
		body.Scope = "global"
	}

	var updatedBy *uuid.UUID
	if ac := auth.FromContext(r.Context()); ac != nil {
		updatedBy = &ac.UserID
	}

	if err := cfg.Store.UpdateSetting(r.Context(), body.Key, body.Value, body.Scope, updatedBy); err != nil {
		WriteError(w, mapSettingsError(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// mapSettingsError maps store errors to appropriate API errors.
func mapSettingsError(err error) *APIError {
	msg := err.Error()
	if strings.Contains(msg, "not found") {
		return ErrNotFound(msg)
	}
	return ErrInternal(msg)
}
