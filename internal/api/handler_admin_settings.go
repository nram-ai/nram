package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	GetSetting(ctx context.Context, key, scope string) (*model.Setting, error)
	UpdateSetting(ctx context.Context, key string, value json.RawMessage, scope string, updatedBy *uuid.UUID) error
	GetSettingsSchema(ctx context.Context) ([]SettingSchema, error)
	GetSettingsGroups(ctx context.Context) ([]SettingGroup, error)

	// ResetSetting reverts a single setting at (key, scope) to its registered
	// default. At scope=="global", performs an upsert with the canonical default
	// value. At any other scope, deletes the row so the cascade resolver falls
	// back to the global default.
	ResetSetting(ctx context.Context, key, scope string, updatedBy *uuid.UUID) error

	// ResetAllSettings reverts every registered schema key at the given scope.
	// Returns the count of keys reset. Atomic at the store boundary.
	ResetAllSettings(ctx context.Context, scope string, updatedBy *uuid.UUID) (int, error)
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
	// OmitFromResetAll excludes this entry from the bulk "reset all to
	// defaults" path so credentials and connection strings (Qdrant address,
	// API keys, provider URLs) are not wiped to their empty schema default
	// when an operator clicks Reset all. Per-key reset still works — the
	// operator has to explicitly target the key. The PUT path is unaffected.
	OmitFromResetAll bool `json:"omit_from_reset_all,omitempty"`
}

// SettingGroup is one tab/card in the admin Settings UI: an ordered set of
// sub-sections, each bound to a setting category. This taxonomy is the single
// source of truth for how the UI organizes settings — the React page renders
// it generically rather than hardcoding the structure. RequiresEnrichment and
// RequiresBackend let the UI hide a whole group when it does not apply to the
// running deployment (enrichment off, or a backend the group is not relevant
// to). Served by GET /admin/settings?groups=true.
type SettingGroup struct {
	ID                 string              `json:"id"`
	Label              string              `json:"label"`
	Description        string              `json:"description,omitempty"`
	RequiresEnrichment bool                `json:"requires_enrichment,omitempty"`
	RequiresBackend    []string            `json:"requires_backend,omitempty"`
	SubSections        []SettingSubSection `json:"subsections"`
}

// SettingSubSection binds one setting category to a heading within a group.
// Label/Description are optional: when a group has a single sub-section whose
// label and description are empty, the UI renders the items flat under the
// group header.
type SettingSubSection struct {
	Category    string `json:"category"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
}

// settingUpdateRequest is the request body for PUT /settings.
type settingUpdateRequest struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
	Scope string          `json:"scope"`
}

// settingResetRequest is the request body for POST /settings/reset. Both
// fields are optional: omitting Key resets every registered key at Scope.
// Omitting Scope defaults to "global".
type settingResetRequest struct {
	Key   string `json:"key,omitempty"`
	Scope string `json:"scope,omitempty"`
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
			switch {
			case r.URL.Query().Get("schema") == "true":
				handleSettingsSchema(w, r, cfg)
			case r.URL.Query().Get("groups") == "true":
				handleSettingsGroups(w, r, cfg)
			default:
				handleListSettings(w, r, cfg)
			}
		case http.MethodPut:
			handleUpdateSetting(w, r, cfg)
		default:
			WriteError(w, ErrBadRequest("method not allowed"))
		}
	}
}

// NewAdminSettingsResetHandler returns an http.HandlerFunc for
// POST /settings/reset. The request body is settingResetRequest; an empty
// body resets every registered key at scope "global". Returns 405-style
// 400 ("method not allowed") for non-POST so the contract matches the
// rest of the admin settings surface.
func NewAdminSettingsResetHandler(cfg SettingsAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}
		handleResetSetting(w, r, cfg)
	}
}

// requireGlobalScope rejects any scope other than "global", writing a 400 and
// returning true when it does (matching requireValidProjectSettings et al.).
// Every admin-managed setting — prompt templates, provider config, ranking
// weights, cost rates, and the rest of the registry — is resolved exclusively
// at global scope. The settings table's cascade machinery (project/user/org) is
// not wired to any of these keys, so a non-global write would persist an orphan
// row that is never read. Genuine per-project/user overrides live in
// projects.settings / users.settings JSON via the cascade resolver, not this
// endpoint. Rejecting the write keeps the failure visible instead of silently dead.
func requireGlobalScope(w http.ResponseWriter, scope string) bool {
	if scope != "global" {
		WriteError(w, ErrBadRequest(fmt.Sprintf("scope %q is not supported; settings are global-only", scope)))
		return true
	}
	return false
}

// handleResetSetting handles POST /settings/reset. An empty key resets every
// registered schema key at the given scope; a non-empty key resets only that
// one. The runtime default comes from the schema registry (DefaultValue) so
// resets stay aligned with the values the editor advertises as "default".
func handleResetSetting(w http.ResponseWriter, r *http.Request, cfg SettingsAdminConfig) {
	body := settingResetRequest{}
	// Read the body once and decode only when there's payload. ContentLength
	// is -1 for chunked / Transfer-Encoded requests, so a length check alone
	// either drops a real chunked body or rejects an empty one with 400; reading
	// the bytes first sidesteps both. Empty body is legal: "reset all at global".
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		WriteError(w, ErrBadRequest("could not read request body"))
		return
	}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			WriteError(w, ErrBadRequest("invalid JSON body"))
			return
		}
	}

	body.Key = strings.TrimSpace(body.Key)
	body.Scope = strings.TrimSpace(body.Scope)
	if body.Scope == "" {
		body.Scope = "global"
	}
	if requireGlobalScope(w, body.Scope) {
		return
	}

	var updatedBy *uuid.UUID
	if ac := auth.FromContext(r.Context()); ac != nil {
		updatedBy = &ac.UserID
	}

	if body.Key != "" {
		// Look up the schema entry: we both validate registration AND use its
		// DefaultValue to re-run the PUT-side validator (min/max bounds and
		// cross-key invariants like high_water > low_water). Without this,
		// resetting one half of an invariant pair while the other is overridden
		// can leave a configuration that PUT would reject.
		schemas, err := cfg.Store.GetSettingsSchema(r.Context())
		if err != nil {
			WriteError(w, mapSettingsError(err))
			return
		}
		var entry *SettingSchema
		for i := range schemas {
			if schemas[i].Key == body.Key {
				entry = &schemas[i]
				break
			}
		}
		if entry == nil {
			WriteError(w, ErrBadRequest(fmt.Sprintf("setting %q is not registered", body.Key)))
			return
		}
		if err := validateValueAgainstSchema(r.Context(), cfg.Store, body.Key, entry.DefaultValue); err != nil {
			WriteError(w, ErrBadRequest(fmt.Sprintf("reset %q would violate an invariant: %v", body.Key, err)))
			return
		}

		if err := cfg.Store.ResetSetting(r.Context(), body.Key, body.Scope, updatedBy); err != nil {
			WriteError(w, mapSettingsError(err))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "reset": 1})
		return
	}

	count, err := cfg.Store.ResetAllSettings(r.Context(), body.Scope, updatedBy)
	if err != nil {
		WriteError(w, mapSettingsError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "reset": count})
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

	writeJSON(w, http.StatusOK, map[string]any{"data": schemas})
}

// handleSettingsGroups handles GET /settings?groups=true — returns the parent-
// group taxonomy the admin UI renders settings into.
func handleSettingsGroups(w http.ResponseWriter, r *http.Request, cfg SettingsAdminConfig) {
	groups, err := cfg.Store.GetSettingsGroups(r.Context())
	if err != nil {
		WriteError(w, mapSettingsError(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"data": groups})
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
	if requireGlobalScope(w, body.Scope) {
		return
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
		if entry.Min != nil || entry.Max != nil {
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
		return validateNumericCrossKeyInvariants(ctx, store, key, value)
	}
	return nil
}

// validateNumericCrossKeyInvariants enforces invariants that span more than
// one setting. Today this catches misconfigurations of the transitive
// high_water / low_water pair: low_water must be strictly less than
// high_water or the pressure-prune in phase_pruning misbehaves (it would
// either never fire or never converge). The check fetches the paired key's
// currently stored value; if absent, it falls back to the registered default.
func validateNumericCrossKeyInvariants(ctx context.Context, store SettingsAdminStore, key string, value json.RawMessage) error {
	switch key {
	case service.SettingDreamTransitiveNamespaceHighWater,
		service.SettingDreamTransitiveNamespaceLowWater:
		incoming, ok := decodeNumeric(value)
		if !ok {
			return nil
		}
		paired := service.SettingDreamTransitiveNamespaceLowWater
		if key == service.SettingDreamTransitiveNamespaceLowWater {
			paired = service.SettingDreamTransitiveNamespaceHighWater
		}
		pairedVal, err := fetchNumericSetting(ctx, store, paired)
		if err != nil {
			return err
		}
		highWater, lowWater := incoming, pairedVal
		if key == service.SettingDreamTransitiveNamespaceLowWater {
			highWater, lowWater = pairedVal, incoming
		}
		if !(lowWater < highWater) {
			return fmt.Errorf("setting %q: namespace_low_water (%v) must be strictly less than namespace_high_water (%v); the pressure-prune drain target must sit below the trigger", key, lowWater, highWater)
		}
	}
	return nil
}

// fetchNumericSetting reads the global-scope value of a numeric setting,
// falling back to the registered default when the row is absent. Used by
// cross-key invariant checks where the incoming PUT must be validated
// against the paired key's current effective value.
func fetchNumericSetting(ctx context.Context, store SettingsAdminStore, key string) (float64, error) {
	setting, err := store.GetSetting(ctx, key, "global")
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return service.GetDefaultFloat(key), nil
		}
		return 0, fmt.Errorf("read paired setting %q: %w", key, err)
	}
	n, ok := decodeNumeric(setting.Value)
	if !ok {
		return 0, fmt.Errorf("paired setting %q: stored value is not numeric", key)
	}
	return n, nil
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
