package api

import (
	"net/http"

	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/service"
)

// rankingWeightKeys is the allow-list of setting keys returned by
// GET /v1/me/ranking-weights/defaults. It mirrors the keys consumed by the
// SPA's SYSTEM_RANKING_WEIGHT_KEYS in ui/src/hooks/useApi.ts. The handler
// returns only these eight keys so the self-tier endpoint never leaks
// unrelated operator-tuned settings to non-admin callers.
var rankingWeightKeys = []string{
	service.SettingRankWeightSim,
	service.SettingRankWeightRec,
	service.SettingRankWeightImp,
	service.SettingRankWeightFreq,
	service.SettingRankWeightGraph,
	service.SettingRankWeightConf,
	service.SettingRankWeightOrigin,
	service.SettingRankWeightMmr,
}

// MeRankingWeightDefault is one row in the response of
// GET /v1/me/ranking-weights/defaults. The Value field is the effective
// global-scope value (operator-set override when present, schema default
// otherwise); the SPA uses it as the placeholder for the per-project
// ranking-weight override editor.
type MeRankingWeightDefault struct {
	Key          string   `json:"key"`
	Value        float64  `json:"value"`
	DefaultValue float64  `json:"default_value"`
	Min          *float64 `json:"min,omitempty"`
	Max          *float64 `json:"max,omitempty"`
	Step         *float64 `json:"step,omitempty"`
}

// MeRankingWeightsDefaultsConfig wires NewMeRankingWeightsDefaultsHandler.
// The Store interface is the same one the admin settings handler depends on;
// no admin-tier gating lives at the store layer (the auth gate sits on the
// /v1/admin route group in router.go), so reusing it here is safe.
type MeRankingWeightsDefaultsConfig struct {
	Store SettingsAdminStore
}

// NewMeRankingWeightsDefaultsHandler returns a GET-only handler at
// /v1/me/ranking-weights/defaults that surfaces the eight ranking.weight.*
// schema entries plus their effective global-scope value to any
// authenticated caller. The per-project Ranking Weights editor in
// ProjectManagement reads from here instead of /admin/settings so non-admin
// project owners (org_owner, member) can render the editor without eating
// 403s on admin routes.
//
// Authentication is required (AuthMiddleware sits above the /v1/me group);
// no role check, matching MeCapabilities.
func NewMeRankingWeightsDefaultsHandler(cfg MeRankingWeightsDefaultsConfig) http.HandlerFunc {
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

		out := make([]MeRankingWeightDefault, 0, len(rankingWeightKeys))
		for _, key := range rankingWeightKeys {
			entry, ok := schemaByKey[key]
			if !ok {
				// Missing schema entry indicates a misalignment between the
				// allow-list above and the registry in settings_store.go.
				// Fail closed; the SPA will surface "schema unavailable" as
				// it does today rather than silently rendering a partial set.
				WriteError(w, ErrInternal("ranking weight schema entry missing: "+key))
				return
			}
			defaultVal, ok := decodeNumeric(entry.DefaultValue)
			if !ok {
				WriteError(w, ErrInternal("ranking weight default is not numeric: "+key))
				return
			}
			effective := defaultVal
			// Operator override at scope=global wins over the schema default.
			// Pre-seeder rows return sql.ErrNoRows from GetSetting; treat any
			// non-numeric or absent override as "no override set" rather than
			// failing the whole response. The schema default is the safe
			// fallback in either case.
			if override, err := cfg.Store.GetSetting(r.Context(), key, "global"); err == nil && override != nil {
				if v, ok := decodeNumeric(override.Value); ok {
					effective = v
				}
			}
			out = append(out, MeRankingWeightDefault{
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
