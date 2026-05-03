package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// settingsListMaxLimit is the default and maximum page size for
// GET /admin/settings. The endpoint is a 1:1 companion to GET
// /admin/settings?schema=true (which is unpaginated): both must always
// reflect every registered key, so the floor needs to clear the whole
// schema registry (~170 entries today) with multi-year headroom. The UI
// requests this same limit explicitly; if the registry ever exceeds it,
// both sides have to grow together.
const settingsListMaxLimit = 500

// SettingsAdminStore abstracts storage operations for the settings admin API.
type SettingsAdminStore interface {
	CountSettings(ctx context.Context, scope string) (int, error)
	ListSettings(ctx context.Context, scope string, limit, offset int) ([]model.Setting, error)
	UpdateSetting(ctx context.Context, key string, value json.RawMessage, scope string, updatedBy *uuid.UUID) error
	GetSettingsSchema(ctx context.Context) ([]SettingSchema, error)
}

// SettingsAdminConfig holds the dependencies for the settings admin handler.
type SettingsAdminConfig struct {
	Store SettingsAdminStore
}

// SettingSchema describes a single setting definition with its type and default.
type SettingSchema struct {
	Key             string          `json:"key"`
	Type            string          `json:"type"`
	DefaultValue    json.RawMessage `json:"default_value"`
	Description     string          `json:"description"`
	Category        string          `json:"category"`
	EnumValues      []string        `json:"enum_values,omitempty"`
	RequiresRestart bool            `json:"requires_restart,omitempty"`
	// AppliesToBackend lets the UI scope a setting to specific runtime
	// backends. Empty (omitted) means "applies regardless of backend". When
	// populated, the UI hides or greys the row when the active backend is
	// not in the list. Recognized values: "sqlite", "postgres", "hnsw",
	// "pgvector", "qdrant".
	AppliesToBackend []string `json:"applies_to_backend,omitempty"`
	// Min and Max are enforced on PUT /admin/settings: values outside the
	// range are rejected with 400 regardless of caller. Step is advisory
	// only — operators may save values between steps. Pointer-typed so
	// omitted (nil) is distinguishable from `Min: 0` in JSON.
	Min  *float64 `json:"min,omitempty"`
	Max  *float64 `json:"max,omitempty"`
	Step *float64 `json:"step,omitempty"`
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

	// The settings registry is bounded compile-time data (~170 entries today)
	// and conceptually a 1:1 companion to GET /settings?schema=true, which is
	// unpaginated. The default page size must comfortably cover the whole
	// registry — otherwise the bootstrap seeder writes rows the UI never
	// reads, and operator changes to keys past the page boundary appear lost
	// even though the PUT succeeded.
	limit := settingsListMaxLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			limit = n
		}
	}
	if limit > settingsListMaxLimit {
		limit = settingsListMaxLimit
	}

	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	total, err := cfg.Store.CountSettings(r.Context(), scope)
	if err != nil {
		WriteError(w, mapSettingsError(err))
		return
	}

	settings, err := cfg.Store.ListSettings(r.Context(), scope, limit, offset)
	if err != nil {
		WriteError(w, mapSettingsError(err))
		return
	}
	if settings == nil {
		settings = []model.Setting{}
	}

	writeJSON(w, http.StatusOK, model.PaginatedResponse[model.Setting]{
		Data: settings,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	})
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

	if err := validateValueAgainstSchema(r.Context(), cfg.Store, body.Key, body.Value); err != nil {
		WriteError(w, ErrBadRequest(err.Error()))
		return
	}

	if err := cfg.Store.UpdateSetting(r.Context(), body.Key, body.Value, body.Scope, updatedBy); err != nil {
		WriteError(w, mapSettingsError(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// validateValueAgainstSchema enforces the schema's Min/Max range on PUT.
// Numeric values outside [Min, Max] are rejected with a 400. The check
// applies regardless of caller — UI, MCP tool, curl, or direct API.
//
// Validation is skipped when:
//   - the key has no schema entry (forward-compat for runtime-only keys
//     that haven't been registered yet);
//   - the schema entry is non-numeric (string, secret, enum, prompt,
//     boolean — those have their own well-formedness checks elsewhere);
//   - neither Min nor Max is set on the schema (treat as unbounded).
//
// String-encoded numeric values (e.g. `"0.5"` instead of `0.5`) are
// accepted and parsed — some HTTP clients round-trip JSON numbers as
// strings, and the runtime resolver tolerates both shapes already.
func validateValueAgainstSchema(ctx context.Context, store SettingsAdminStore, key string, value json.RawMessage) error {
	schemas, err := store.GetSettingsSchema(ctx)
	if err != nil {
		// Schema lookup itself failed. The in-process schema slice cannot
		// realistically error — but if a future store implementation can,
		// fail closed: a write we cannot validate is a write we should not
		// accept.
		return fmt.Errorf("schema lookup failed: %w", err)
	}
	var entry *SettingSchema
	for i := range schemas {
		if schemas[i].Key == key {
			entry = &schemas[i]
			break
		}
	}
	if entry == nil {
		return nil
	}
	switch entry.Type {
	case "json":
		return validateJSONSettingValue(entry.Key, value)
	case "number":
		if entry.Min == nil && entry.Max == nil {
			return nil
		}
		n, ok := decodeNumeric(value)
		if !ok {
			return fmt.Errorf("setting %q: value must be a number", key)
		}
		if entry.Min != nil && n < *entry.Min {
			return fmt.Errorf("setting %q: value %v is below schema minimum %v", key, n, *entry.Min)
		}
		if entry.Max != nil && n > *entry.Max {
			return fmt.Errorf("setting %q: value %v is above schema maximum %v", key, n, *entry.Max)
		}
	}
	return nil
}

// validateJSONSettingValue dispatches per-key validation for Type:"json"
// entries. Unknown json keys pass through so a new key can be registered
// before its validator ships.
func validateJSONSettingValue(key string, value json.RawMessage) error {
	switch key {
	case service.SettingTokenCostRates:
		return validateCostRatesValue(value)
	}
	return nil
}

// costRateEntry mirrors the SPA-side CostRate type for shape validation
// on PUT; runtime callers parse the raw blob themselves.
type costRateEntry struct {
	Key             string  `json:"key"`
	InputCostPer1k  float64 `json:"inputCostPer1k"`
	OutputCostPer1k float64 `json:"outputCostPer1k"`
}

func validateCostRatesValue(value json.RawMessage) error {
	var rates []costRateEntry
	if err := json.Unmarshal(value, &rates); err != nil {
		return fmt.Errorf("setting %q: value must be a JSON array of {key, inputCostPer1k, outputCostPer1k} objects: %v", service.SettingTokenCostRates, err)
	}
	seen := make(map[string]struct{}, len(rates))
	for i, r := range rates {
		if strings.TrimSpace(r.Key) == "" {
			return fmt.Errorf("setting %q: entry %d has empty key", service.SettingTokenCostRates, i)
		}
		if _, dup := seen[r.Key]; dup {
			return fmt.Errorf("setting %q: duplicate entry for key %q", service.SettingTokenCostRates, r.Key)
		}
		seen[r.Key] = struct{}{}
		if r.InputCostPer1k < 0 {
			return fmt.Errorf("setting %q: entry %q has negative inputCostPer1k", service.SettingTokenCostRates, r.Key)
		}
		if r.OutputCostPer1k < 0 {
			return fmt.Errorf("setting %q: entry %q has negative outputCostPer1k", service.SettingTokenCostRates, r.Key)
		}
	}
	return nil
}

// decodeNumeric tolerates both bare JSON numbers (`0.5`) and JSON-encoded
// numeric strings (`"0.5"`). Returns the parsed float and ok=true on
// success; ok=false if the raw value is neither shape.
func decodeNumeric(raw json.RawMessage) (float64, bool) {
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// mapSettingsError maps store errors to appropriate API errors.
func mapSettingsError(err error) *APIError {
	msg := err.Error()
	if strings.Contains(msg, "not found") {
		return ErrNotFound(msg)
	}
	return ErrInternal(msg)
}
