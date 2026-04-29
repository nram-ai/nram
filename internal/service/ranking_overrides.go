package service

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// ProjectRankingOverride captures sparse per-project overrides for the system
// RankingWeights cascade. Each field is a pointer so unset fields fall through
// to the base weights; only set fields contribute to the merged result.
type ProjectRankingOverride struct {
	Similarity     *float64
	Recency        *float64
	Importance     *float64
	Frequency      *float64
	GraphRelevance *float64
	Confidence     *float64
}

// rankingOverrideKey maps incoming JSON keys to the override field they
// populate. The legacy `relevance` alias is honoured for projects whose
// settings JSON predates the canonical migration to `similarity`.
var rankingOverrideKey = map[string]string{
	"similarity":      "Similarity",
	"recency":         "Recency",
	"importance":      "Importance",
	"frequency":       "Frequency",
	"graph_relevance": "GraphRelevance",
	"confidence":      "Confidence",
	"relevance":       "Similarity",
}

// ParseRankingOverride decodes the project.Settings.ranking_weights subtree
// into a sparse override. Tolerates legacy `relevance` (maps to Similarity),
// drops unknown keys, returns a single error reporting every offending field
// when any recognised value is non-finite or outside [0.0, 1.0]. When both
// `similarity` and the legacy `relevance` are present, `similarity` wins.
func ParseRankingOverride(raw json.RawMessage) (ProjectRankingOverride, error) {
	var ov ProjectRankingOverride
	if len(raw) == 0 || string(raw) == "null" {
		return ov, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return ov, fmt.Errorf("ranking_weights: %w", err)
	}

	var errs []string
	legacyRelevance := math.NaN()
	hasCanonicalSimilarity := false

	for key, value := range fields {
		field, ok := rankingOverrideKey[key]
		if !ok {
			continue
		}
		var f float64
		if err := json.Unmarshal(value, &f); err != nil {
			errs = append(errs, fmt.Sprintf("%s: not a number", key))
			continue
		}
		if err := ValidateUnitFloat(key, f); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		switch field {
		case "Similarity":
			if key == "relevance" {
				legacyRelevance = f
			} else {
				hasCanonicalSimilarity = true
				ov.Similarity = ptrTo(f)
			}
		case "Recency":
			ov.Recency = ptrTo(f)
		case "Importance":
			ov.Importance = ptrTo(f)
		case "Frequency":
			ov.Frequency = ptrTo(f)
		case "GraphRelevance":
			ov.GraphRelevance = ptrTo(f)
		case "Confidence":
			ov.Confidence = ptrTo(f)
		}
	}

	if !hasCanonicalSimilarity && !math.IsNaN(legacyRelevance) {
		v := legacyRelevance
		ov.Similarity = &v
	}

	if len(errs) > 0 {
		return ov, fmt.Errorf("ranking_weights: %s", strings.Join(errs, "; "))
	}
	return ov, nil
}

// MergeWeights returns base with each non-nil override field replaced.
func MergeWeights(base RankingWeights, ov ProjectRankingOverride) RankingWeights {
	out := base
	if ov.Similarity != nil {
		out.Similarity = *ov.Similarity
	}
	if ov.Recency != nil {
		out.Recency = *ov.Recency
	}
	if ov.Importance != nil {
		out.Importance = *ov.Importance
	}
	if ov.Frequency != nil {
		out.Frequency = *ov.Frequency
	}
	if ov.GraphRelevance != nil {
		out.GraphRelevance = *ov.GraphRelevance
	}
	if ov.Confidence != nil {
		out.Confidence = *ov.Confidence
	}
	return out
}

func ptrTo[T any](v T) *T {
	return &v
}
