package service

import (
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage"
)

// DefaultRRFConstant is the canonical k from Cormack, Clarke, and Buettcher
// (2009): "Reciprocal Rank Fusion outperforms Condorcet and individual rank
// learning methods." It dampens contributions from rows deep in any single
// ranking, preventing one channel's tail from overpowering another channel's
// head.
const DefaultRRFConstant = 60

// ReciprocalRankFusion fuses N ranked lists into a single score per document.
// For each ranking i and each document d at 1-based position rank_i(d):
//
//	score(d) += weights[i] / (k + rank_i(d))
//
// Documents present in only some rankings receive contributions only from
// those rankings (unranked == omitted, not == ranked-last). The returned map
// is keyed by uuid.UUID; the absolute magnitudes are unitless and only
// meaningful for ordering — callers needing a [0, 1] score should normalize
// by the map's max value.
//
// k <= 0 is coerced to DefaultRRFConstant. weights with len != len(rankings)
// is coerced to uniform 1.0 across all rankings (plain RRF).
func ReciprocalRankFusion(rankings [][]storage.MemoryRank, k int, weights []float64) map[uuid.UUID]float64 {
	if k <= 0 {
		k = DefaultRRFConstant
	}
	if len(weights) != len(rankings) {
		weights = make([]float64, len(rankings))
		for i := range weights {
			weights[i] = 1.0
		}
	}

	scores := make(map[uuid.UUID]float64)
	for i, ranking := range rankings {
		w := weights[i]
		for pos, row := range ranking {
			// 1-based rank position so the head of the list contributes
			// 1/(k+1) — the canonical RRF formulation.
			scores[row.ID] += w / float64(k+pos+1)
		}
	}
	return scores
}
