package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
)

// CostRatesStore is the read surface for the global token-cost-rates
// blob. Writes go through the admin settings PUT and stay gated by
// RequireRole(Administrator); this read is open to any authenticated
// user so non-admins can see dollar columns in their usage breakdown.
type CostRatesStore interface {
	GetCostRates(ctx context.Context) (json.RawMessage, error)
}

type CostRatesConfig struct {
	Store CostRatesStore
}

// NewUsageCostRatesHandler serves GET /v1/usage/cost_rates. A missing
// row collapses to an empty list rather than 404; fresh deployments
// haven't run the bootstrap seeder yet, and the SPA must never have to
// special-case "no rates configured."
func NewUsageCostRatesHandler(cfg CostRatesConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}
		raw, err := cfg.Store.GetCostRates(r.Context())
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			WriteError(w, ErrInternal(err.Error()))
			return
		}
		if len(raw) == 0 {
			raw = json.RawMessage("[]")
		}
		writeJSON(w, http.StatusOK, map[string]json.RawMessage{"data": raw})
	}
}
