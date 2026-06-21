package enrichment

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/nram-ai/nram/internal/cluster"
	"github.com/nram-ai/nram/internal/provider"
)

// sentenceBoundary matches a sentence-ending punctuation run followed by
// whitespace. Splitting only at punctuation-then-space means a boundary can
// never fall inside a token, so decimals and ratios ("12-15%", "0.6b" has no
// trailing space) are never cut. Abbreviations are handled separately.
var sentenceBoundary = regexp.MustCompile(`([.!?]+)\s+`)

// abbreviations are lowercased tokens that commonly precede a period without
// ending a sentence; a boundary right after one is suppressed so "et al. (2024)"
// and "e.g. X" stay whole.
var abbreviations = map[string]bool{
	"al": true, "etc": true, "vs": true, "e.g": true, "i.e": true,
	"dr": true, "mr": true, "mrs": true, "ms": true, "fig": true,
	"no": true, "cf": true, "inc": true, "ltd": true, "st": true,
	"approx": true, "est": true,
}

// dottedAcronym matches multi-token acronyms whose internal periods are not
// sentence boundaries: "U.S", "U.K", "a.m", "Ph.D" (as seen just before the
// final boundary period, which is not part of the captured token). The
// single-token abbreviations map cannot catch these because the word before the
// boundary still carries an internal dot. Each segment is one or two letters,
// which is tight enough that ordinary prose (no internal dots) never matches.
var dottedAcronym = regexp.MustCompile(`^[A-Za-z]{1,2}(?:\.[A-Za-z]{1,2})+$`)

// SplitSentences breaks text into sentences using a conservative heuristic:
// split at sentence-ending punctuation followed by whitespace, but not when the
// preceding word is a known abbreviation. Boundaries only ever fall between
// tokens, so no decimal, ratio, or version string is split mid-token. Returned
// sentences are trimmed and non-empty; input with no boundary returns one
// element (the whole trimmed text) when non-empty, else nil.
func SplitSentences(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	locs := sentenceBoundary.FindAllStringSubmatchIndex(text, -1)
	var out []string
	start := 0
	for _, loc := range locs {
		// loc[0]=match start (the punctuation), loc[1]=match end (after spaces).
		punctStart := loc[0]
		// The word immediately before the punctuation.
		prev := text[start:punctStart]
		lastWord := prev
		if i := strings.LastIndexAny(lastWord, " \t\n"); i >= 0 {
			lastWord = lastWord[i+1:]
		}
		if abbreviations[strings.ToLower(strings.TrimRight(lastWord, "."))] || dottedAcronym.MatchString(lastWord) {
			continue // not a real boundary; keep accumulating
		}
		seg := strings.TrimSpace(text[start:loc[1]])
		if seg != "" {
			out = append(out, seg)
		}
		start = loc[1]
	}
	if tail := strings.TrimSpace(text[start:]); tail != "" {
		out = append(out, tail)
	}
	return out
}

// ExtractFacets computes the multi-vector facet set for a memory. facet 0 is the
// caller-supplied pooled whole-memory embedding (wholeVec); topic facets are the
// pooled embeddings of cosine-clustered sentences. A coherent memory (one
// cluster, too few sentences, or too short) yields just [wholeVec], so faceting
// is self-limiting. At most maxFacets vectors are returned (facet 0 plus up to
// maxFacets-1 topic facets), preferring the largest clusters.
//
// embedder is used to embed the individual sentences at the given dimension. A
// nil/empty wholeVec is an error; sentence-embedding failures are returned.
func ExtractFacets(ctx context.Context, embedder provider.EmbeddingProvider, content string, wholeVec []float32, dim int, threshold float64, maxFacets int) ([][]float32, error) {
	if len(wholeVec) == 0 {
		return nil, fmt.Errorf("enrichment: ExtractFacets requires a non-empty whole-memory vector")
	}
	facets := [][]float32{wholeVec}
	if maxFacets <= 1 || embedder == nil {
		return facets, nil
	}
	sentences := SplitSentences(content)
	if len(sentences) < 2 {
		return facets, nil
	}
	resp, err := embedder.Embed(provider.WithOperation(ctx, provider.OperationFacetEmbedding), &provider.EmbeddingRequest{Input: sentences, Dimension: dim})
	if err != nil {
		return nil, fmt.Errorf("enrichment: embed sentences for facets: %w", err)
	}
	if len(resp.Embeddings) != len(sentences) {
		return nil, fmt.Errorf("enrichment: facet embed returned %d vectors for %d sentences", len(resp.Embeddings), len(sentences))
	}

	clusters := cluster.AnchorClusters(resp.Embeddings, threshold)
	if len(clusters) <= 1 {
		// Single coherent topic: the pooled whole-memory vector already covers it.
		return facets, nil
	}
	// Prefer the largest clusters when capping; ties keep input (anchor) order.
	sort.SliceStable(clusters, func(i, j int) bool { return len(clusters[i]) > len(clusters[j]) })
	for _, members := range clusters {
		if len(facets) >= maxFacets {
			break
		}
		vecs := make([][]float32, len(members))
		for i, idx := range members {
			vecs[i] = resp.Embeddings[idx]
		}
		if pooled := cluster.Pool(vecs); pooled != nil {
			facets = append(facets, pooled)
		}
	}
	return facets, nil
}
