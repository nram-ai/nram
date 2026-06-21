package enrichment

import (
	"context"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/provider"
)

// fakeFacetEmbedder embeds each input by a caller-supplied mapping to a fixed
// axis, so tests control which sentences are near (same axis) or far (different
// axes) in cosine space.
type fakeFacetEmbedder struct {
	dim     int
	axisFor func(s string) int
	err     error
}

func (f *fakeFacetEmbedder) Name() string      { return "fake" }
func (f *fakeFacetEmbedder) Dimensions() []int { return []int{f.dim} }
func (f *fakeFacetEmbedder) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([][]float32, len(req.Input))
	for i, s := range req.Input {
		v := make([]float32, f.dim)
		v[f.axisFor(s)%f.dim] = 1
		out[i] = v
	}
	return &provider.EmbeddingResponse{Embeddings: out}, nil
}

func TestSplitSentences(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "   ", nil},
		{"single", "Just one sentence with no terminal punctuation", []string{"Just one sentence with no terminal punctuation"}},
		{"two", "First sentence. Second sentence.", []string{"First sentence.", "Second sentence."}},
		{"abbrev et al", "Kooij et al. (2024) found an effect. The next claim follows.", []string{"Kooij et al. (2024) found an effect.", "The next claim follows."}},
		{"decimal not split", "Pace is 3.76 lbs per week now. Two escalations remain.", []string{"Pace is 3.76 lbs per week now.", "Two escalations remain."}},
		{"eg abbrev", "Use a model, e.g. qwen3, for this. Then embed it.", []string{"Use a model, e.g. qwen3, for this.", "Then embed it."}},
		{"dotted acronym US", "The U.S. economy grew this year. Inflation then fell.", []string{"The U.S. economy grew this year.", "Inflation then fell."}},
		{"dotted acronym phd", "She earned a Ph.D. in physics last spring. The lab celebrated.", []string{"She earned a Ph.D. in physics last spring.", "The lab celebrated."}},
		// Conservative tradeoff (matches the abbreviations map): an acronym at a
		// genuine sentence end under-splits rather than risking a mid-token cut.
		{"acronym at sentence end merges", "He left at 9 a.m. We started without him.", []string{"He left at 9 a.m. We started without him."}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SplitSentences(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d sentences %q, want %d %q", len(got), got, len(tc.want), tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("sentence %d = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestExtractFacets_RequiresWholeVec(t *testing.T) {
	_, err := ExtractFacets(context.Background(), nil, "x", nil, 8, 0.9, 4)
	if err == nil {
		t.Fatal("expected error for empty whole vector")
	}
}

func TestExtractFacets_SingleTopicReturnsOnlyPooled(t *testing.T) {
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	emb := &fakeFacetEmbedder{dim: dim, axisFor: func(string) int { return 3 }} // all sentences same axis -> one cluster
	facets, err := ExtractFacets(context.Background(), emb, "One topic here. Still the same topic. Yet more.", whole, dim, 0.9, 4)
	if err != nil {
		t.Fatalf("ExtractFacets: %v", err)
	}
	if len(facets) != 1 {
		t.Fatalf("single-topic should yield 1 facet (pooled), got %d", len(facets))
	}
}

func TestExtractFacets_MultiTopicSplitsByCluster(t *testing.T) {
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	// Sentences containing "PRICE" -> axis 1; "DEPLOY" -> axis 5.
	emb := &fakeFacetEmbedder{dim: dim, axisFor: func(s string) int {
		if strings.Contains(s, "PRICE") {
			return 1
		}
		return 5
	}}
	content := "PRICE one. PRICE two. PRICE three. DEPLOY one. DEPLOY two."
	facets, err := ExtractFacets(context.Background(), emb, content, whole, dim, 0.9, 4)
	if err != nil {
		t.Fatalf("ExtractFacets: %v", err)
	}
	// facet 0 (pooled) + 2 topic facets.
	if len(facets) != 3 {
		t.Fatalf("expected 3 facets (pooled + 2 topics), got %d", len(facets))
	}
	if facets[0][0] != 1 {
		t.Errorf("facet 0 should be the supplied whole vector")
	}
	// Largest cluster (PRICE, 3 sentences) comes first among topic facets.
	if facets[1][1] == 0 {
		t.Errorf("first topic facet should be the PRICE cluster (axis 1), got %v", facets[1])
	}
}

func TestExtractFacets_MaxFacetsCap(t *testing.T) {
	dim := 8
	whole := make([]float32, dim)
	whole[0] = 1
	// Each sentence a distinct axis -> many singleton clusters.
	axis := 0
	emb := &fakeFacetEmbedder{dim: dim, axisFor: func(string) int { axis = (axis + 1) % dim; return axis }}
	content := "Aa one. Bb two. Cc three. Dd four. Ee five."
	facets, err := ExtractFacets(context.Background(), emb, content, whole, dim, 0.99, 3)
	if err != nil {
		t.Fatalf("ExtractFacets: %v", err)
	}
	if len(facets) > 3 {
		t.Fatalf("maxFacets=3 not honored, got %d facets", len(facets))
	}
}
