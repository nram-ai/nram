package api

import (
	"net/http"

	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/service"
)

// settingDefaultKeys is the allow-list of setting keys returned by
// GET /v1/me/setting-defaults. These are non-sensitive numeric settings that
// general-user pages need to render against the operator-configured defaults
// without reading the admin-only /v1/admin/settings schema. Keeping this an
// explicit allow-list (rather than echoing the whole registry) ensures the
// self-tier endpoint never leaks unrelated operator-tuned settings to
// non-admin callers.
//
// The graph layout keys drive GraphVisualization (a general-user page whose
// per-project Layout drawer persists through /v1/me/projects). The dedup
// threshold drives the placeholder on the per-project / per-user override
// inputs in ProjectManagement and UserManagement.
var settingDefaultKeys = []string{
	service.SettingGraphCenterGravity,
	service.SettingGraphChargeStrength,
	service.SettingGraphLinkDistance,
	service.SettingDedupThreshold,
}

// MeSettingDefault is one row in the response of GET /v1/me/setting-defaults.
// Value is the effective global-scope value (operator-set override when
// present, schema default otherwise); the SPA uses it as the live default for
// the corresponding control. DefaultValue is the registered schema default,
// exposed separately so the SPA can distinguish "operator changed it" from the
// shipped baseline. Min/Max/Step come straight from the schema entry.
type MeSettingDefault struct {
	Key          string   `json:"key"`
	Value        float64  `json:"value"`
	DefaultValue float64  `json:"default_value"`
	Min          *float64 `json:"min,omitempty"`
	Max          *float64 `json:"max,omitempty"`
	Step         *float64 `json:"step,omitempty"`
}

// MeSettingDefaultsConfig wires NewMeSettingDefaultsHandler. The Store
// interface is the same one the admin settings handler depends on; no
// admin-tier gating lives at the store layer (the auth gate sits on the
// /v1/admin route group in router.go), so reusing it for a self-tier read is
// safe.
type MeSettingDefaultsConfig struct {
	Store SettingsAdminStore
}

// NewMeSettingDefaultsHandler returns a GET-only handler at
// /v1/me/setting-defaults that surfaces the allow-listed numeric setting
// schema entries plus their effective global-scope value to any authenticated
// caller. It mirrors NewMeRankingWeightsDefaultsHandler: general-user pages
// (the graph visualization page, the per-project override editors) read from
// here instead of /admin/settings so non-admin owners (org_owner, member) can
// render against the real operator defaults without eating 403s on admin
// routes.
//
// Authentication is required (AuthMiddleware sits above the /v1/me group); no
// role check, matching MeCapabilities and MeRankingWeightsDefaults.
func NewMeSettingDefaultsHandler(cfg MeSettingDefaultsConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			WriteError(w, &APIError{
				Code:    "method_not_allowed",
				Message: "method not allowed",
				Status:  http.StatusMethodNotAllowed,
			})
			return
		}

		if auth.FromContext(r.Context()) == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		if cfg.Store == nil {
			WriteError(w, ErrInternal("settings store unavailable"))
			return
		}

		schemas, err := cfg.Store.GetSettingsSchema(r.Context())
		if err != nil {
			WriteError(w, mapSettingsError(err))
			return
		}
		schemaByKey := make(map[string]*SettingSchema, len(schemas))
		for i := range schemas {
			schemaByKey[schemas[i].Key] = &schemas[i]
		}

		out := make([]MeSettingDefault, 0, len(settingDefaultKeys))
		for _, key := range settingDefaultKeys {
			entry, ok := schemaByKey[key]
			if !ok {
				// Missing schema entry indicates a misalignment between the
				// allow-list above and the registry in settings_store.go. Fail
				// closed rather than silently returning a partial set.
				WriteError(w, ErrInternal("setting schema entry missing: "+key))
				return
			}
			defaultVal, ok := decodeNumeric(entry.DefaultValue)
			if !ok {
				WriteError(w, ErrInternal("setting default is not numeric: "+key))
				return
			}
			effective := defaultVal
			// Operator override at scope=global wins over the schema default.
			// Treat any non-numeric or absent override as "no override set"
			// rather than failing the whole response; the schema default is the
			// safe fallback in either case.
			if override, err := cfg.Store.GetSetting(r.Context(), key, "global"); err == nil && override != nil {
				if v, ok := decodeNumeric(override.Value); ok {
					effective = v
				}
			}
			out = append(out, MeSettingDefault{
				Key:          key,
				Value:        effective,
				DefaultValue: defaultVal,
				Min:          entry.Min,
				Max:          entry.Max,
				Step:         entry.Step,
			})
		}

		writeJSON(w, http.StatusOK, map[string]any{"data": out})
	}
}
