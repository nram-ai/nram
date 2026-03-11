package enrichment

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Dependency-inversion interface for vector similarity search
// ---------------------------------------------------------------------------

// VectorSearcher provides vector similarity search within a namespace.
type VectorSearcher interface {
	Search(ctx context.Context, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]storage.VectorSearchResult, error)
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// DeduplicationConfig controls the behaviour of the deduplication check.
type DeduplicationConfig struct {
	Threshold float64 // similarity threshold; matches >= this value are duplicates (default 0.92)
	TopK      int     // number of nearest-neighbour candidates to evaluate (default 5)
}

// DefaultDeduplicationConfig provides sensible defaults for deduplication.
var DefaultDeduplicationConfig = DeduplicationConfig{
	Threshold: 0.92,
	TopK:      5,
}

func (c DeduplicationConfig) withDefaults() DeduplicationConfig {
	if c.Threshold <= 0 {
		c.Threshold = DefaultDeduplicationConfig.Threshold
	}
	if c.TopK <= 0 {
		c.TopK = DefaultDeduplicationConfig.TopK
	}
	return c
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

// DeduplicationResult describes whether a piece of content is a duplicate of
// an existing memory.
type DeduplicationResult struct {
	IsDuplicate bool       // true when the closest match meets or exceeds the threshold
	SimilarID   *uuid.UUID // ID of the most similar existing memory, nil if no match
	Similarity  float64    // similarity score of the closest match
}

// ---------------------------------------------------------------------------
// Deduplicator
// ---------------------------------------------------------------------------

// Deduplicator checks whether incoming content duplicates memories that
// already exist in the same namespace by comparing embedding vectors.
type Deduplicator struct {
	vectorStore   VectorSearcher
	embedProvider func() provider.EmbeddingProvider
	config        DeduplicationConfig
}

// NewDeduplicator constructs a Deduplicator. The embedProvider function may
// return nil to indicate that embedding is unavailable; in that case every
// check returns not-duplicate.
func NewDeduplicator(
	vectorStore VectorSearcher,
	embedProvider func() provider.EmbeddingProvider,
	config DeduplicationConfig,
) *Deduplicator {
	return &Deduplicator{
		vectorStore:   vectorStore,
		embedProvider: embedProvider,
		config:        config.withDefaults(),
	}
}

// Check determines whether content is a duplicate of an existing memory in
// the given namespace. If no embedding provider is available the result is
// always not-duplicate.
func (d *Deduplicator) Check(ctx context.Context, content string, namespaceID uuid.UUID) (*DeduplicationResult, error) {
	ep := d.embedProvider()
	if ep == nil {
		return &DeduplicationResult{}, nil
	}

	// Embed the incoming content.
	resp, err := ep.Embed(ctx, &provider.EmbeddingRequest{
		Input: []string{content},
	})
	if err != nil {
		return nil, fmt.Errorf("dedup: embed content: %w", err)
	}
	if len(resp.Embeddings) == 0 {
		return nil, fmt.Errorf("dedup: embed returned no vectors")
	}

	embedding := resp.Embeddings[0]
	dimension := len(embedding)

	// Search for nearest neighbours in the namespace.
	results, err := d.vectorStore.Search(ctx, embedding, namespaceID, dimension, d.config.TopK)
	if err != nil {
		return nil, fmt.Errorf("dedup: vector search: %w", err)
	}

	if len(results) == 0 {
		return &DeduplicationResult{}, nil
	}

	// The vector store returns results ordered by descending similarity; the
	// first element is the closest match.
	best := results[0]
	id := best.ID
	res := &DeduplicationResult{
		SimilarID:  &id,
		Similarity: best.Score,
	}
	if best.Score >= d.config.Threshold {
		res.IsDuplicate = true
	}
	return res, nil
}

// CheckBatch evaluates multiple content strings for duplication in a single
// call. It batch-embeds the inputs, then performs an individual vector search
// for each. Results are returned in the same order as the inputs.
func (d *Deduplicator) CheckBatch(ctx context.Context, contents []string, namespaceID uuid.UUID) ([]DeduplicationResult, error) {
	if len(contents) == 0 {
		return nil, nil
	}

	ep := d.embedProvider()
	if ep == nil {
		out := make([]DeduplicationResult, len(contents))
		return out, nil
	}

	// Batch embed all contents.
	resp, err := ep.Embed(ctx, &provider.EmbeddingRequest{
		Input: contents,
	})
	if err != nil {
		return nil, fmt.Errorf("dedup batch: embed: %w", err)
	}
	if len(resp.Embeddings) != len(contents) {
		return nil, fmt.Errorf("dedup batch: expected %d embeddings, got %d", len(contents), len(resp.Embeddings))
	}

	out := make([]DeduplicationResult, len(contents))
	for i, emb := range resp.Embeddings {
		dimension := len(emb)

		results, err := d.vectorStore.Search(ctx, emb, namespaceID, dimension, d.config.TopK)
		if err != nil {
			return nil, fmt.Errorf("dedup batch: vector search for item %d: %w", i, err)
		}

		if len(results) == 0 {
			continue
		}

		best := results[0]
		id := best.ID
		out[i].SimilarID = &id
		out[i].Similarity = best.Score
		if best.Score >= d.config.Threshold {
			out[i].IsDuplicate = true
		}
	}

	return out, nil
}
