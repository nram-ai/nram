package enrichment

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Dependency-inversion interface for vector similarity search
// ---------------------------------------------------------------------------

// VectorSearcher provides vector similarity search within a namespace.
type VectorSearcher interface {
	Search(ctx context.Context, kind storage.VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]storage.VectorSearchResult, error)
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

// MemoryMatch is one near-neighbour candidate returned by FindNearMatches:
// the matched memory's identity, its content, the similarity score, and its
// creation time. The ingestion-decision phase feeds these into an LLM judge,
// so content and created_at are required, not just the ID.
type MemoryMatch struct {
	ID        uuid.UUID
	Content   string
	Score     float64
	CreatedAt time.Time
}

// ---------------------------------------------------------------------------
// Deduplicator
// ---------------------------------------------------------------------------

// Deduplicator checks whether incoming content duplicates memories that
// already exist in the same namespace by comparing embedding vectors.
type Deduplicator struct {
	vectorStore   VectorSearcher
	embedProvider func() provider.EmbeddingProvider
	memories      MemoryReader
	config        DeduplicationConfig
}

// NewDeduplicator constructs a Deduplicator. The embedProvider function may
// return nil to indicate that embedding is unavailable; in that case every
// check returns not-duplicate. memories may be nil for callers that only use
// Check / CheckBatch (which return IDs only); FindNearMatches needs it to
// hydrate matched memory content for the LLM judge.
func NewDeduplicator(
	vectorStore VectorSearcher,
	embedProvider func() provider.EmbeddingProvider,
	memories MemoryReader,
	config DeduplicationConfig,
) *Deduplicator {
	return &Deduplicator{
		vectorStore:   vectorStore,
		embedProvider: embedProvider,
		memories:      memories,
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
	results, err := d.vectorStore.Search(ctx, storage.VectorKindMemory, embedding, namespaceID, dimension, d.config.TopK)
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

		results, err := d.vectorStore.Search(ctx, storage.VectorKindMemory, emb, namespaceID, dimension, d.config.TopK)
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

// FindNearMatches returns up to topK existing memories in the namespace whose
// embeddings exceed threshold similarity to the supplied embedding. Each match
// includes the existing memory's content and created_at so the caller can feed
// the candidates into an LLM judge. Results are sorted by descending score.
//
// excludeID, when non-nil, is filtered out of the result set defensively. The
// vector store does not exclude the just-stored memory by id today, so a
// caller that searches with the new memory's own embedding could otherwise
// self-match if the vector store had been populated.
//
// Threshold and topK passed here override the configured defaults so the
// enrichment-time call can be tuned independently of the Check path.
func (d *Deduplicator) FindNearMatches(
	ctx context.Context,
	embedding []float32,
	namespaceID uuid.UUID,
	topK int,
	threshold float64,
	excludeID *uuid.UUID,
) ([]MemoryMatch, error) {
	if len(embedding) == 0 {
		return nil, nil
	}
	if topK <= 0 {
		topK = d.config.TopK
	}
	if threshold <= 0 {
		threshold = d.config.Threshold
	}

	// Over-fetch by one when an exclude is specified so a self-match does
	// not silently shrink the returned set below topK.
	fetch := topK
	if excludeID != nil {
		fetch = topK + 1
	}

	results, err := d.vectorStore.Search(ctx, storage.VectorKindMemory, embedding, namespaceID, len(embedding), fetch)
	if err != nil {
		return nil, fmt.Errorf("dedup: find near matches: vector search: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}

	// First pass: filter by threshold + exclude, capped at topK. Doing this
	// before the hydrate query keeps the IN (...) lookup proportional to
	// what we will return rather than to the full search width.
	kept := make([]storage.VectorSearchResult, 0, len(results))
	for _, r := range results {
		if r.Score < threshold {
			continue
		}
		if excludeID != nil && r.ID == *excludeID {
			continue
		}
		kept = append(kept, r)
		if len(kept) >= topK {
			break
		}
	}
	if len(kept) == 0 {
		return nil, nil
	}

	// Hydrate all surviving IDs in one batch read. Vectors that point at a
	// missing or soft-deleted memory row drop out silently (GetBatch
	// excludes deleted_at IS NOT NULL).
	hydrated := map[uuid.UUID]model.Memory{}
	if d.memories != nil {
		ids := make([]uuid.UUID, len(kept))
		for i, r := range kept {
			ids[i] = r.ID
		}
		mems, err := d.memories.GetBatch(ctx, ids)
		if err != nil {
			return nil, fmt.Errorf("dedup: hydrate matches: %w", err)
		}
		for i := range mems {
			hydrated[mems[i].ID] = mems[i]
		}
	}

	out := make([]MemoryMatch, 0, len(kept))
	for _, r := range kept {
		match := MemoryMatch{ID: r.ID, Score: r.Score}
		if mem, ok := hydrated[r.ID]; ok {
			match.Content = mem.Content
			match.CreatedAt = mem.CreatedAt
		} else if d.memories != nil {
			// Hydration was attempted but the row is gone. Skip rather
			// than expose an empty-content candidate to the LLM judge.
			continue
		}
		out = append(out, match)
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out, nil
}
