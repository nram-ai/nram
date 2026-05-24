package service

import "math"

// mmrSelect performs Maximal Marginal Relevance (MMR) reranking on the
// composite-ordered `passing` list, producing up to `windowSize` results that
// trade off query relevance against redundancy with already-selected siblings.
//
// Algorithm (greedy MMR, Carbonell and Goldstein 1998):
//
//	score(d) = lambda * sim_to_query(d) - (1 - lambda) * max_{d' in selected} sim(d, d')
//
// At each step pick the unselected candidate maximizing score(d), append to
// selected, repeat until every embedded candidate has been selected.
//
// sim_to_query is resolved per-candidate by relevanceForQuery: prefer the raw
// cosine on RecallResult.Similarity (vector channel evidence), otherwise
// compute cosine of the candidate's hydrated embedding against the query
// embedding on the fly so lexical-only / shared-namespace candidates compete
// on the same [-1, 1] cosine scale instead of mixing in the composite Score
// (which carries recency, importance, confidence, and origin terms unrelated
// to query similarity). Score is the last-resort fallback only when the
// candidate has no vector evidence at all.
//
// Candidates split by whether their embedding is hydrated:
//   - Embedded candidates participate in the MMR loop and fill the slots they
//     occupied in composite order with whichever MMR pick is next in line.
//   - Missing-embedding candidates stay anchored to their composite-rank
//     position in the output: a high-composite lexical-only or unbackfilled-
//     embedding hit at passing[0] still appears at output[0]. MMR neither
//     scores them against siblings (no embedding to score with) nor demotes
//     them to the tail.
//
// Fast paths:
//   - lambda >= 1.0: pure relevance; return passing[:min(windowSize, len)].
//   - lambda <= 0.0: misconfigured (pure-diversity mode is not implemented);
//     treat as bypass, same truncation as lambda >= 1.0.
//   - len(passing) <= 1 or windowSize <= 0: return passing unchanged or
//     truncated, with no rerank.
//   - Fewer than two embedded candidates: MMR is degenerate (no pairwise
//     comparison possible); fall through to plain truncation.
//
// The first MMR pick is the highest-composite-score embedded candidate (which
// is the first embedded entry of passing, since passing is already sorted by
// composite score upstream). The composite score already incorporates
// similarity plus six other terms; treating its winner as the first MMR
// selection preserves the upstream ranking's top decision and only engages
// MMR's diversity penalty from the second selection forward.
func mmrSelect(passing []RecallResult, queryEmbedding []float32, lambda float64, windowSize int) []RecallResult {
	if windowSize <= 0 {
		return passing
	}
	if windowSize > len(passing) {
		windowSize = len(passing)
	}
	if len(passing) <= 1 {
		return passing[:windowSize]
	}
	if lambda >= 1.0 || lambda <= 0.0 {
		return passing[:windowSize]
	}

	embedded := make([]int, 0, len(passing))
	for i, r := range passing {
		if len(r.embedding) > 0 {
			embedded = append(embedded, i)
		}
	}

	if len(embedded) < 2 {
		return passing[:windowSize]
	}

	selected := make([]int, 0, len(embedded))
	used := make(map[int]bool, len(embedded))
	selected = append(selected, embedded[0])
	used[embedded[0]] = true

	for len(selected) < len(embedded) {
		bestIdx := -1
		bestScore := math.Inf(-1)
		for _, i := range embedded {
			if used[i] {
				continue
			}
			simQ := relevanceForQuery(passing[i], queryEmbedding)
			maxSimSel := math.Inf(-1)
			for _, j := range selected {
				s := cosineSim(passing[i].embedding, passing[j].embedding)
				if s > maxSimSel {
					maxSimSel = s
				}
			}
			score := lambda*simQ - (1.0-lambda)*maxSimSel
			if score > bestScore {
				bestScore = score
				bestIdx = i
			}
		}
		if bestIdx == -1 {
			break
		}
		selected = append(selected, bestIdx)
		used[bestIdx] = true
	}

	// Assemble the output by walking passing in original composite-rank order.
	// Missing-embedding rows are emitted as-is at their composite position so
	// a high-composite lexical-only or unbackfilled hit is not demoted to the
	// tail. Embedded positions get filled with the next MMR-ranked pick from
	// selected, so the embedded subset is reordered without displacing rows
	// that MMR could not score.
	out := make([]RecallResult, 0, windowSize)
	embedCursor := 0
	for p := 0; p < len(passing) && len(out) < windowSize; p++ {
		if len(passing[p].embedding) == 0 {
			out = append(out, passing[p])
			continue
		}
		if embedCursor < len(selected) {
			out = append(out, passing[selected[embedCursor]])
			embedCursor++
		}
	}
	return out
}

// relevanceForQuery resolves the query-similarity term used by mmrSelect for
// a single candidate. The preference order is raw cosine, on-the-fly cosine
// against the query embedding, then composite Score; this keeps every
// embedded candidate on the same [-1, 1] cosine scale and only falls back
// to the multi-factor composite when there is no vector evidence at all.
func relevanceForQuery(r RecallResult, queryEmbedding []float32) float64 {
	if r.Similarity != nil {
		return *r.Similarity
	}
	if len(r.embedding) > 0 && len(queryEmbedding) == len(r.embedding) {
		return cosineSim(r.embedding, queryEmbedding)
	}
	return r.Score
}

// cosineSim returns the cosine similarity between two float32 vectors. Zero-
// length, zero-norm, or length-mismatched inputs return 0 rather than NaN so
// the MMR redundancy penalty stays well-defined for degenerate inputs. Real
// cosines can be negative (anti-correlated embeddings), and the caller MUST
// initialize its running max with math.Inf(-1) rather than 0 so a negative
// cosine actually wins the max comparison instead of being silently floored.
func cosineSim(a, b []float32) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		normA += x * x
		normB += y * y
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}
