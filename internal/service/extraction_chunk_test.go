package service

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/provider"
)

// defaultSettings resolves every key to settingDefaults: mockSettingsRepo with
// nothing put returns sql.ErrNoRows, which is what the cascade falls back on.
// NewSettingsService(nil) is not a substitute: a nil repo panics in Resolve.
func defaultSettings() *SettingsService { return NewSettingsService(newMockSettingsRepo()) }

// settingsWith overrides the given keys at global scope; everything else falls
// through to settingDefaults.
func settingsWith(over map[string]string) *SettingsService {
	repo := newMockSettingsRepo()
	for k, v := range over {
		repo.put(k, "global", v)
	}
	return NewSettingsService(repo)
}

// uniform builds n repetitions of evenly-tokenizing prose.
func uniform(n int) string {
	return strings.TrimSpace(strings.Repeat("alpha beta gamma delta epsilon ", n))
}

// denseThenSparse builds a document whose leading region tokenizes far denser
// than its trailing region. The document-wide tokens-per-word average that sizes
// the word window therefore badly under-estimates the leading region, which is
// what pushes a window past the threshold.
//
// Every word is distinct, deliberately. Overlap assertions depend on it: over
// repeated text a "does the next window's first word appear in this one" check
// is satisfied by any occurrence of that word anywhere, so it would pass whether
// or not the windows actually overlap.
func denseThenSparse() string {
	var b strings.Builder
	for i := range 900 {
		fmt.Fprintf(&b, "k%04d!@#$%%^&*()_+ ", i)
	}
	for i := range 700 {
		fmt.Fprintf(&b, "w%04dx ", i)
	}
	return strings.TrimSpace(b.String())
}

// reversedWords flips word order, turning a dense-prefix document into a
// dense-suffix one so both ends of the density profile get exercised.
func reversedWords(s string) string {
	w := strings.Fields(s)
	for i, j := 0, len(w)-1; i < j; i, j = i+1, j-1 {
		w[i], w[j] = w[j], w[i]
	}
	return strings.Join(w, " ")
}

// normalizedFitsButRawDoesNot reproduces the live shape of f8295d29: the raw
// content is over the threshold (so chunking triggers) while the same content
// with whitespace collapsed is under it. chunkExtractionContent measures the raw
// string but builds windows from strings.Fields, so the two disagree. That gap
// is load-bearing: it is why this memory gets two windows, and the A/B
// measured it at 113 edges with two windows against 4 with one.
func normalizedFitsButRawDoesNot() string {
	var b strings.Builder
	for range 380 {
		b.WriteString("alpha beta gamma delta epsilon zeta\n\n")
	}
	return strings.TrimSpace(b.String())
}

// TestChunkExtractionContent_ChunkNeverExceedsThreshold is the core invariant:
// the threshold exists to bound what one extraction call is handed, so an
// emitted chunk may never exceed it. Uneven token density used to defeat the
// average-based word window entirely.
func TestChunkExtractionContent_ChunkNeverExceedsThreshold(t *testing.T) {
	ctx := context.Background()
	s := defaultSettings()
	threshold := s.ResolveIntWithDefault(ctx, SettingExtractionChunkThresholdTokens, "global")

	for _, tc := range []struct {
		name    string
		content string
	}{
		{"uniform density", uniform(2000)},
		{"dense prefix, sparse suffix", denseThenSparse()},
		{"sparse prefix, dense suffix", reversedWords(denseThenSparse())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks := chunkExtractionContent(ctx, s, tc.content)
			if len(chunks) < 2 {
				t.Fatalf("fixture did not chunk (%d chunks); it must exceed the threshold to exercise this", len(chunks))
			}
			for i, c := range chunks {
				if got := provider.EstimateTokens("", c); got > threshold {
					t.Errorf("chunk %d/%d is %d tokens, exceeds threshold %d", i, len(chunks), got, threshold)
				}
			}
		})
	}
}

// TestChunkExtractionContent_SplitPreservesOverlap guards the reason overlap
// exists: an entity or relation straddling a boundary must survive intact in at
// least one chunk. A window split for being oversize must not introduce a new
// zero-overlap seam.
//
// The fixture uses distinct words deliberately, and the subtest below forces
// overlap to 0 to prove the assertion can actually observe its absence; on
// repeated text this check passes no matter what the chunker does.
func TestChunkExtractionContent_SplitPreservesOverlap(t *testing.T) {
	ctx := context.Background()
	content := denseThenSparse()

	// seams reports how many adjacent chunk boundaries are NOT covered by overlap.
	seams := func(s *SettingsService) (total, uncovered, chunks int) {
		cs := chunkExtractionContent(ctx, s, content)
		for i := 0; i < len(cs)-1; i++ {
			total++
			next := strings.Fields(cs[i+1])
			if len(next) == 0 {
				t.Fatalf("empty chunk at %d", i+1)
			}
			if !strings.Contains(cs[i], next[0]) {
				uncovered++
			}
		}
		return total, uncovered, len(cs)
	}

	t.Run("default overlap covers every seam", func(t *testing.T) {
		total, uncovered, n := seams(defaultSettings())
		if n < 2 {
			t.Fatalf("fixture must chunk to exercise seams, got %d chunks", n)
		}
		if uncovered != 0 {
			t.Errorf("%d of %d seams (across %d chunks) have no overlap; a split introduced an uncovered boundary",
				uncovered, total, n)
		}
	})

	// Without this the assertion above is untrustworthy: it must fail when the
	// property it claims to test is removed.
	t.Run("assertion detects missing overlap", func(t *testing.T) {
		zero := settingsWith(map[string]string{SettingExtractionChunkOverlapTokens: "0"})
		total, uncovered, n := seams(zero)
		if n < 2 {
			t.Fatalf("fixture must chunk, got %d chunks", n)
		}
		if uncovered == 0 {
			t.Fatalf("with overlap forced to 0, all %d seams still looked covered: the overlap assertion is vacuous", total)
		}
	})
}

// TestChunkExtractionContent_NeverReducesChunkCount pins the load-bearing
// property: each chunk is a separate LLM call with its own max_tokens, so the
// chunk count IS the extraction output budget. Reducing it silently reduces how
// many relationships a dense memory can yield.
func TestChunkExtractionContent_NeverReducesChunkCount(t *testing.T) {
	ctx := context.Background()
	s := defaultSettings()
	threshold := s.ResolveIntWithDefault(ctx, SettingExtractionChunkThresholdTokens, "global")

	for _, tc := range []struct {
		name    string
		content string
		min     int // an explicit floor where ceil(total/threshold) is too slack to bite
	}{
		{name: "uniform density", content: uniform(2000)},
		{name: "dense prefix, sparse suffix", content: denseThenSparse()},
		// The regression this guard exists for: greedy repacking measured the
		// normalized text, found it under the threshold, and collapsed this shape
		// to a single window. ceil(total/threshold) is 2 here, so the floor bites.
		{name: "normalized fits but raw does not", content: normalizedFitsButRawDoesNot(), min: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			total := provider.EstimateTokens("", tc.content)
			want := max((total+threshold-1)/threshold, tc.min) // ceil, with an explicit floor
			got := len(chunkExtractionContent(ctx, s, tc.content))
			if got < want {
				t.Errorf("got %d chunks for %d raw tokens at threshold %d; must not drop below %d (chunk count is the output budget)",
					got, total, threshold, want)
			}
		})
	}
}

// TestChunkExtractionContent_UnderThresholdIsSingleChunkVerbatim covers the
// overwhelmingly common path: a small memory is passed through untouched, with
// its original whitespace, not re-joined from Fields.
func TestChunkExtractionContent_UnderThresholdIsSingleChunkVerbatim(t *testing.T) {
	ctx := context.Background()
	s := defaultSettings()
	content := "A short memory.\n\nWith   irregular\tspacing preserved."
	chunks := chunkExtractionContent(ctx, s, content)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0] != content {
		t.Errorf("under-threshold content was rewritten:\n got %q\nwant %q", chunks[0], content)
	}
}

func TestChunkExtractionContent_NilSettingsAndZeroThreshold(t *testing.T) {
	ctx := context.Background()
	big := uniform(2000)

	if chunks := chunkExtractionContent(ctx, nil, big); len(chunks) != 1 || chunks[0] != big {
		t.Errorf("nil settings must disable chunking, got %d chunks", len(chunks))
	}

	zero := settingsWith(map[string]string{SettingExtractionChunkThresholdTokens: "0"})
	if chunks := chunkExtractionContent(ctx, zero, big); len(chunks) != 1 || chunks[0] != big {
		t.Errorf("zero threshold must disable chunking, got %d chunks", len(chunks))
	}
}

// TestChunkExtractionContent_OverlapExceedingChunkFallsBack covers the guard
// where a misconfigured overlap is >= the whole chunk: it must fall back rather
// than produce a zero/negative step and stall.
func TestChunkExtractionContent_OverlapExceedingChunkFallsBack(t *testing.T) {
	ctx := context.Background()
	s := settingsWith(map[string]string{
		SettingExtractionChunkThresholdTokens: "300",
		SettingExtractionChunkOverlapTokens:   "5000", // far larger than a chunk
	})
	content := uniform(500)
	chunks := chunkExtractionContent(ctx, s, content)
	if len(chunks) < 2 {
		t.Fatalf("expected chunking, got %d chunks", len(chunks))
	}
	for i, c := range chunks {
		if got := provider.EstimateTokens("", c); got > 300 {
			t.Errorf("chunk %d is %d tokens, exceeds threshold 300", i, got)
		}
	}
	// Chunks must cover the document: the last chunk has to reach the final word.
	last := strings.Fields(content)
	if !strings.Contains(chunks[len(chunks)-1], last[len(last)-1]) {
		t.Error("chunking did not reach the end of the content")
	}
}

// --- merge / relationship-pass coverage ---

func TestMergeRelationEnvelope_DedupesAcrossChunks(t *testing.T) {
	seen := map[string]bool{}
	a := &RelationExtractionEnvelope{
		Result: &RelationExtractionResult{Relationships: []ExtractedRelation{
			{Source: "nram", Relation: "uses", Target: "postgres"},
			{Source: "nram", Relation: "uses", Target: "qdrant"},
		}},
		Model:        "m1",
		ProviderName: "p1",
		Usage:        provider.TokenUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}
	// Same triple in different case/spacing must collapse; one new triple must land.
	b := &RelationExtractionEnvelope{
		Result: &RelationExtractionResult{Relationships: []ExtractedRelation{
			{Source: "NRAM", Relation: "Uses", Target: " postgres "},
			{Source: "nram", Relation: "uses", Target: "sglang"},
		}},
		Usage: provider.TokenUsage{PromptTokens: 7, CompletionTokens: 3, TotalTokens: 10},
	}

	merged := mergeRelationEnvelope(nil, a, seen)
	merged = mergeRelationEnvelope(merged, b, seen)

	if got := len(merged.Result.Relationships); got != 3 {
		t.Fatalf("expected 3 deduped relationships, got %d: %+v", got, merged.Result.Relationships)
	}
	if merged.Model != "m1" || merged.ProviderName != "p1" {
		t.Errorf("identity not seeded from first envelope: model=%q provider=%q", merged.Model, merged.ProviderName)
	}
	if merged.Usage.TotalTokens != 25 {
		t.Errorf("usage not accumulated across chunks: got %d, want 25", merged.Usage.TotalTokens)
	}
}

func TestExtractRelationshipsLLM_MultiChunkMergesAndDedupes(t *testing.T) {
	ctx := context.Background()
	// Disable continuation so the call count reflects chunks alone.
	s := settingsWith(map[string]string{SettingExtractionContinuationMaxPasses: "0"})
	content := denseThenSparse()
	wantChunks := len(chunkExtractionContent(ctx, s, content))
	if wantChunks < 2 {
		t.Fatalf("fixture must chunk, got %d", wantChunks)
	}

	// Every chunk proposes the same shared edge; each also proposes a unique one.
	replies := make([]string, wantChunks)
	for i := range replies {
		replies[i] = fmt.Sprintf(
			`{"relationships":[{"source":"nram","relation":"uses","target":"postgres"},`+
				`{"source":"nram","relation":"uses","target":"chunk%d"}]}`, i)
	}
	llm := &askSeqLLM{replies: replies}

	env, err := ExtractRelationshipsLLM(ctx, llm, s, content, []string{"nram", "postgres"}, CallOptions{MaxTokens: 512})
	if err != nil {
		t.Fatalf("ExtractRelationshipsLLM: %v", err)
	}
	if llm.calls != wantChunks {
		t.Errorf("expected one call per chunk (%d), got %d", wantChunks, llm.calls)
	}
	// The shared edge collapses to one; each chunk's unique edge survives.
	want := 1 + wantChunks
	if got := len(env.Result.Relationships); got != want {
		t.Errorf("expected %d merged relationships (1 shared + %d unique), got %d: %+v",
			want, wantChunks, got, env.Result.Relationships)
	}
}

func TestExtractRelationshipsLLM_EmptyEntityNamesShortCircuits(t *testing.T) {
	ctx := context.Background()
	llm := &mockLLMProvider{name: "mock"}
	env, err := ExtractRelationshipsLLM(ctx, llm, defaultSettings(), "some content", nil, CallOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env == nil || env.Result == nil || len(env.Result.Relationships) != 0 {
		t.Errorf("expected an empty result envelope, got %+v", env)
	}
	if llm.called != 0 {
		t.Errorf("expected 0 provider calls, got %d", llm.called)
	}
}
