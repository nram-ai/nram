package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/nram-ai/nram/internal/service"
)

// requireValidProjectSettings validates the JSON and writes a 400 if it fails.
// Returns true when the response was already written and the handler must
// return; false when the caller should proceed.
func requireValidProjectSettings(w http.ResponseWriter, raw json.RawMessage) bool {
	if err := ValidateProjectSettingsJSON(raw); err != nil {
		WriteError(w, ErrBadRequest(err.Error()))
		return true
	}
	return false
}

// requireValidUserSettings is the user-scope counterpart. Returns true when
// the response was already written.
func requireValidUserSettings(w http.ResponseWriter, raw json.RawMessage) bool {
	if err := ValidateUserSettingsJSON(raw); err != nil {
		WriteError(w, ErrBadRequest(err.Error()))
		return true
	}
	return false
}

// settingsRoot mirrors the JSON shape that project.Settings and user.Settings
// serialize. Only fields wired through the cascade are validated here; any
// other keys (legacy or bespoke) are passed through untouched.
type settingsRoot struct {
	RankingWeights    json.RawMessage `json:"ranking_weights"`
	DedupThreshold    json.RawMessage `json:"dedup_threshold"`
	EnrichmentEnabled json.RawMessage `json:"enrichment_enabled"`
}

// ValidateProjectSettingsJSON parses the validatable fields of a project's
// settings blob and returns the first parse error. Non-empty input that does
// not decode as a JSON object is rejected outright. An empty / nil payload
// is allowed (no validation needed).
func ValidateProjectSettingsJSON(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var root settingsRoot
	if err := json.Unmarshal(raw, &root); err != nil {
		return fmt.Errorf("settings: %w", err)
	}
	if _, err := service.ParseRankingOverride(root.RankingWeights); err != nil {
		return err
	}
	if _, err := service.ParseDedupOverride(root.DedupThreshold); err != nil {
		return err
	}
	if _, err := service.ParseEnrichmentEnabledOverride(root.EnrichmentEnabled); err != nil {
		return err
	}
	return nil
}

// errUserRankingWeightsNotSupported is returned when a user-scoped settings
// payload includes ranking_weights. The cascade lands at project, not user;
// allowing the field would be write-only data with no observable effect.
var errUserRankingWeightsNotSupported = errors.New("ranking_weights: not supported at user scope (use project settings instead)")

// ValidateUserSettingsJSON validates user.settings. It rejects ranking_weights
// presence entirely and applies the same dedup_threshold + enrichment_enabled
// validation as the project handler.
func ValidateUserSettingsJSON(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var root settingsRoot
	if err := json.Unmarshal(raw, &root); err != nil {
		return fmt.Errorf("settings: %w", err)
	}
	if len(root.RankingWeights) > 0 && string(root.RankingWeights) != "null" {
		return errUserRankingWeightsNotSupported
	}
	if _, err := service.ParseDedupOverride(root.DedupThreshold); err != nil {
		return err
	}
	if _, err := service.ParseEnrichmentEnabledOverride(root.EnrichmentEnabled); err != nil {
		return err
	}
	return nil
}
