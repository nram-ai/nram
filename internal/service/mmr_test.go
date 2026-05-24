package service

import (
	"math"
	"testing"
)

func ptrF64(v float64) *float64 { return &v }

func TestCosineSim_DegenerateInputs(t *testing.T) {
	cases := []struct {
		name string
		a, b []float32
		want float64
	}{
		{"both empty", nil, nil, 0},
		{"a empty", nil, []float32{1, 0, 0}, 0},
		{"b empty", []float32{1, 0, 0}, nil, 0},
		{"length mismatch", []float32{1, 0}, []float32{1, 0, 0}, 0},
		{"a zero norm", []float32{0, 0, 0}, []float32{1, 0, 0}, 0},
		{"b zero norm", []float32{1, 0, 0}, []float32{0, 0, 0}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cosineSim(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("cosineSim(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestCosineSim_KnownValues(t *testing.T) {
	cases := []struct {
		name string
		a, b []float32
		want float64
		tol  float64
	}{
		{"identical", []float32{1, 2, 3}, []float32{1, 2, 3}, 1.0, 1e-9},
		{"orthogonal", []float32{1, 0, 0}, []float32{0, 1, 0}, 0.0, 1e-9},
		{"opposite", []float32{1, 0, 0}, []float32{-1, 0, 0}, -1.0, 1e-9},
		{"scaled identical", []float32{2, 4, 6}, []float32{1, 2, 3}, 1.0, 1e-9},
		{"45 degrees", []float32{1, 0}, []float32{1, 1}, math.Sqrt2 / 2, 1e-9},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cosineSim(tc.a, tc.b)
			if math.Abs(got-tc.want) > tc.tol {
				t.Errorf("cosineSim(%v, %v) = %v, want %v (+/- %v)", tc.a, tc.b, got, tc.want, tc.tol)
			}
		})
	}
}

// resultAt builds a minimal RecallResult with the fields mmrSelect reads.
// content doubles as a stable identifier for assertions.
func resultAt(content string, score float64, simToQuery *float64, embedding []float32) RecallResult {
	return RecallResult{
		Content:    content,
		Score:      score,
		Similarity: simToQuery,
		embedding:  embedding,
	}
}

func contents(rs []RecallResult) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Content
	}
	return out
}

func TestMmrSelect_EmptyInput(t *testing.T) {
	got := mmrSelect(nil, nil, 0.75, 10)
	if len(got) != 0 {
		t.Errorf("empty input should return empty, got %d results", len(got))
	}
}

func TestMmrSelect_SingleElement(t *testing.T) {
	in := []RecallResult{resultAt("solo", 0.9, ptrF64(0.9), []float32{1, 0, 0})}
	got := mmrSelect(in, nil, 0.5, 10)
	if len(got) != 1 || got[0].Content != "solo" {
		t.Errorf("single-element input should return the input, got %+v", got)
	}
}

func TestMmrSelect_NonPositiveWindowReturnsInput(t *testing.T) {
	in := []RecallResult{
		resultAt("a", 0.9, ptrF64(0.9), []float32{1, 0, 0}),
		resultAt("b", 0.8, ptrF64(0.8), []float32{0, 1, 0}),
	}
	got := mmrSelect(in, nil, 0.5, 0)
	if len(got) != len(in) {
		t.Errorf("windowSize=0 should return input unchanged, got %d", len(got))
	}
	got = mmrSelect(in, nil, 0.5, -1)
	if len(got) != len(in) {
		t.Errorf("negative windowSize should return input unchanged, got %d", len(got))
	}
}

func TestMmrSelect_LambdaOneFastPath(t *testing.T) {
	// lambda 1.0 should preserve composite order: passing comes in already
	// sorted by composite score, so the result is passing[:window].
	in := []RecallResult{
		resultAt("first", 0.9, ptrF64(0.9), []float32{1, 0, 0}),
		resultAt("second", 0.85, ptrF64(0.85), []float32{0.99, 0.01, 0}), // near-dup of first
		resultAt("third", 0.6, ptrF64(0.6), []float32{0, 1, 0}),
	}
	got := mmrSelect(in, nil, 1.0, 2)
	want := []string{"first", "second"}
	if !equalContents(contents(got), want) {
		t.Errorf("lambda 1.0 should preserve composite order, got %v want %v", contents(got), want)
	}
}

func TestMmrSelect_LambdaZeroFastPath(t *testing.T) {
	// lambda 0.0 disables MMR (pure-diversity mode is not implemented; the
	// schema description documents 0.0 as a disable, matching this fast path).
	in := []RecallResult{
		resultAt("first", 0.9, ptrF64(0.9), []float32{1, 0, 0}),
		resultAt("second", 0.85, ptrF64(0.85), []float32{0.99, 0.01, 0}),
		resultAt("third", 0.6, ptrF64(0.6), []float32{0, 1, 0}),
	}
	got := mmrSelect(in, nil, 0.0, 2)
	want := []string{"first", "second"}
	if !equalContents(contents(got), want) {
		t.Errorf("lambda 0.0 should bypass MMR, got %v want %v", contents(got), want)
	}
}

func TestMmrSelect_DemotesNearDuplicate(t *testing.T) {
	// Cluster: a, b, c, d are near-identical (cosine ~0.98 to each other),
	// e is unrelated. With lambda 0.5 and windowSize 3, MMR should pick one
	// representative from the cluster and leave room for e instead of
	// returning the top-3 of the cluster.
	in := []RecallResult{
		resultAt("a", 0.85, ptrF64(0.85), []float32{1, 0.1, 0}),
		resultAt("b", 0.84, ptrF64(0.84), []float32{1, 0.12, 0}),
		resultAt("c", 0.83, ptrF64(0.83), []float32{1, 0.11, 0}),
		resultAt("d", 0.82, ptrF64(0.82), []float32{1, 0.13, 0}),
		resultAt("e", 0.65, ptrF64(0.65), []float32{0, 1, 0}),
	}
	got := mmrSelect(in, nil, 0.5, 3)
	if len(got) != 3 {
		t.Fatalf("expected 3 results, got %d", len(got))
	}
	// First pick is always the composite-score winner. The cluster has four
	// members above e in composite score, so the first pick is one of them
	// (specifically "a", the highest-composite cluster member).
	if got[0].Content != "a" {
		t.Errorf("first pick should be the composite winner 'a', got %q", got[0].Content)
	}
	// Second pick: e (unrelated) should outrank the remaining cluster members
	// because they each have a 0.98+ redundancy penalty against the selected
	// "a", while e has near-zero overlap. At lambda 0.5:
	//   cluster member b: 0.5 * 0.84 - 0.5 * ~0.99 ~= -0.075
	//   unrelated e:      0.5 * 0.65 - 0.5 * ~0.10 ~=  0.275
	// e wins.
	if got[1].Content != "e" {
		t.Errorf("second pick should be unrelated 'e' (cluster is redundant), got %q", got[1].Content)
	}
}

func TestMmrSelect_WindowLargerThanInputDoesNotPanic(t *testing.T) {
	in := []RecallResult{
		resultAt("a", 0.9, ptrF64(0.9), []float32{1, 0, 0}),
		resultAt("b", 0.8, ptrF64(0.8), []float32{0, 1, 0}),
	}
	got := mmrSelect(in, nil, 0.5, 100)
	if len(got) != 2 {
		t.Errorf("window larger than input should clamp to len, got %d", len(got))
	}
}

func TestMmrSelect_FewerThanTwoEmbeddedFallsThrough(t *testing.T) {
	// One embedded candidate plus a missing-embedding candidate. With only
	// one embedded, MMR is degenerate and the helper falls through to
	// composite-order truncation (same as lambda 1.0 path).
	in := []RecallResult{
		resultAt("first", 0.9, ptrF64(0.9), []float32{1, 0, 0}),
		resultAt("missing", 0.8, ptrF64(0.8), nil),
	}
	got := mmrSelect(in, nil, 0.5, 2)
	want := []string{"first", "missing"}
	if !equalContents(contents(got), want) {
		t.Errorf("single-embedded should fall through to composite order, got %v want %v", contents(got), want)
	}
}

func TestMmrSelect_PreservesHighCompositeMissingEmbedding(t *testing.T) {
	// Regression scenario: passing[0] is a lexical-only or unbackfilled hit
	// at the top of composite ranking with no embedding hydrated; passing[1..3]
	// are embedded near-duplicates. The earlier "pad missing at tail" strategy
	// demoted the rank-0 row to the end of the output, where the final-limit
	// slice could truncate it out. The current helper anchors missing rows at
	// their composite-rank position so the high-composite hit survives.
	in := []RecallResult{
		resultAt("missing_high", 0.95, nil, nil),
		resultAt("emb_a", 0.85, ptrF64(0.85), []float32{1, 0.1, 0}),
		resultAt("emb_b", 0.80, ptrF64(0.80), []float32{1, 0.11, 0}),
		resultAt("emb_c", 0.75, ptrF64(0.75), []float32{1, 0.12, 0}),
	}
	got := mmrSelect(in, nil, 0.5, 4)
	if len(got) != 4 {
		t.Fatalf("expected 4 results, got %d", len(got))
	}
	if got[0].Content != "missing_high" {
		t.Errorf("missing-embedding high-composite candidate should stay at position 0, got %q", got[0].Content)
	}
	// Slots 1-3 are the embedded subset in MMR order; the exact ordering of
	// emb_a/b/c after MMR varies with their pairwise similarities, but every
	// embedded candidate must appear exactly once.
	gotSet := map[string]bool{}
	for _, r := range got[1:] {
		gotSet[r.Content] = true
	}
	for _, want := range []string{"emb_a", "emb_b", "emb_c"} {
		if !gotSet[want] {
			t.Errorf("embedded candidate %q missing from slots 1-3, got %v", want, contents(got[1:]))
		}
	}
}

func TestMmrSelect_MissingEmbeddingsAnchoredAtTailComposite(t *testing.T) {
	// Mirror of the high-composite case: a low-composite missing-embedding
	// candidate stays at its tail composite-rank position rather than being
	// promoted into the MMR-reordered embedded slots.
	in := []RecallResult{
		resultAt("emb_a", 0.9, ptrF64(0.9), []float32{1, 0.1, 0}),
		resultAt("emb_b", 0.85, ptrF64(0.85), []float32{1, 0.11, 0}),
		resultAt("missing_low", 0.5, ptrF64(0.5), nil),
	}
	got := mmrSelect(in, nil, 0.5, 3)
	if len(got) != 3 {
		t.Fatalf("expected 3 results, got %d", len(got))
	}
	if got[0].Content != "emb_a" {
		t.Errorf("first slot should be embedded composite winner emb_a, got %q", got[0].Content)
	}
	if got[2].Content != "missing_low" {
		t.Errorf("missing-embedding candidate should stay at position 2, got %q", got[2].Content)
	}
}

func TestMmrSelect_UsesSimilarityNotScoreForRelevance(t *testing.T) {
	// MMR's relevance term is sim_to_query (RecallResult.Similarity when set),
	// not the composite Score. Three orthogonal embeddings (no redundancy
	// penalty between any pair) so the second pick is determined by relevance
	// alone. Input is in composite-score order so 'a' is the seed.
	//   a (seed): Score 0.95, Similarity 0.5
	//   b:        Score 0.40, Similarity 0.90  <- highest sim_to_query
	//   c:        Score 0.60, Similarity 0.30  <- highest Score among remaining
	// If MMR used Score for relevance, second pick would be c (0.60 > 0.40).
	// Since MMR uses Similarity, second pick should be b (0.90 > 0.30).
	in := []RecallResult{
		resultAt("a", 0.95, ptrF64(0.5), []float32{1, 0, 0}),
		resultAt("c", 0.60, ptrF64(0.3), []float32{0, 0, 1}),
		resultAt("b", 0.40, ptrF64(0.9), []float32{0, 1, 0}),
	}
	got := mmrSelect(in, nil, 0.5, 2)
	if len(got) != 2 || got[0].Content != "a" {
		t.Fatalf("expected seed 'a' first, got %v", contents(got))
	}
	if got[1].Content != "b" {
		t.Errorf("second pick should be 'b' (sim_to_query winner), got %q; MMR is reading Score instead of Similarity", got[1].Content)
	}
}

func TestMmrSelect_UsesQueryEmbeddingWhenSimilarityIsNil(t *testing.T) {
	// When a candidate has no Similarity pointer but DOES have a hydrated
	// embedding (a lexical-only fusion hit with backfilled embedding), MMR
	// computes cosine to the query embedding on the fly so every embedded
	// candidate competes on the same [-1, 1] cosine scale. Without this
	// fallback, mmrSelect would mix raw cosine and composite Score (which
	// carries non-similarity terms) inside the relevance side of the formula.
	//
	// Setup: three orthogonal embeddings (zero pairwise penalty), no
	// Similarity pointers, query embedding aligned with 'b' so cosine(b, q)
	// is the highest. Score values are set inversely so the test fails if
	// MMR falls back to Score (which would prefer 'c').
	queryEmb := []float32{0, 1, 0}
	in := []RecallResult{
		resultAt("a", 0.95, nil, []float32{1, 0, 0}),
		resultAt("c", 0.60, nil, []float32{0, 0, 1}),
		resultAt("b", 0.40, nil, []float32{0, 1, 0}),
	}
	got := mmrSelect(in, queryEmb, 0.5, 2)
	if len(got) != 2 || got[0].Content != "a" {
		t.Fatalf("expected seed 'a' first, got %v", contents(got))
	}
	if got[1].Content != "b" {
		t.Errorf("second pick should be 'b' (cosine 1.0 with query), got %q; relevance fallback isn't reading the query embedding", got[1].Content)
	}
}

func TestMmrSelect_NegativeCosineDoesNotClampToZero(t *testing.T) {
	// Anti-correlated embeddings produce negative cosines. The MMR formula's
	// diversity term (1-lambda) * max_sim_to_selected should reward a
	// candidate with a strongly negative cosine to the selected set; if the
	// running max is initialized to 0.0 it silently clamps negative values
	// and drops the dissimilarity bonus.
	//
	// Setup: 'a' seeded (composite winner). Then 'opposite' is anti-
	// correlated with 'a' (cosine = -1) and has moderate relevance. 'near'
	// is similar to 'a' (cosine = +0.95) with the same relevance. With a
	// proper negative-aware max, opposite wins second pick because its
	// redundancy penalty is -(1-lambda)*(-1) = +(1-lambda), a positive
	// contribution. With a 0.0 floor, opposite's penalty is treated as 0
	// and the two candidates tie on relevance plus 0 penalty, so the order
	// would be ambiguous or favor whichever appears first.
	in := []RecallResult{
		resultAt("a", 0.90, ptrF64(0.90), []float32{1, 0, 0}),
		resultAt("near", 0.60, ptrF64(0.60), []float32{1, 0, 0}),    // cosine to a = 1.0
		resultAt("opposite", 0.60, ptrF64(0.60), []float32{-1, 0, 0}), // cosine to a = -1.0
	}
	got := mmrSelect(in, nil, 0.5, 2)
	if len(got) != 2 || got[0].Content != "a" {
		t.Fatalf("expected seed 'a' first, got %v", contents(got))
	}
	if got[1].Content != "opposite" {
		t.Errorf("second pick should be 'opposite' (negative-cosine diversity bonus), got %q; maxSimSel may be clamping negatives", got[1].Content)
	}
}

func equalContents(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
