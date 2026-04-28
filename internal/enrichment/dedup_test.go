package enrichment

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Mock implementations for dedup tests
// ---------------------------------------------------------------------------

// mockVectorSearcher returns pre-configured search results.
type mockVectorSearcher struct {
	results []storage.VectorSearchResult
	err     error
}

func (m *mockVectorSearcher) Search(_ context.Context, _ storage.VectorKind, _ []float32, _ uuid.UUID, _ int, _ int) ([]storage.VectorSearchResult, error) {
	return m.results, m.err
}

// dedupMockEmbedder returns deterministic embeddings for dedup tests.
type dedupMockEmbedder struct {
	embeddings [][]float32
	err        error
}

func (m *dedupMockEmbedder) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	// Return pre-configured embeddings; if fewer are configured than requested,
	// replicate the first one.
	out := make([][]float32, len(req.Input))
	for i := range req.Input {
		if i < len(m.embeddings) {
			out[i] = m.embeddings[i]
		} else if len(m.embeddings) > 0 {
			out[i] = m.embeddings[0]
		} else {
			out[i] = []float32{0.1, 0.2, 0.3}
		}
	}
	return &provider.EmbeddingResponse{
		Embeddings: out,
		Model:      "mock-embed",
	}, nil
}

func (m *dedupMockEmbedder) Name() string       { return "mock" }
func (m *dedupMockEmbedder) Dimensions() []int   { return []int{3} }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCheck_Duplicate(t *testing.T) {
	similarID := uuid.New()
	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: similarID, Score: 0.95, NamespaceID: uuid.New()},
		},
	}
	ep := &dedupMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	res, err := dedup.Check(context.Background(), "some content", uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsDuplicate {
		t.Error("expected IsDuplicate=true for score 0.95 >= threshold 0.92")
	}
	if res.SimilarID == nil || *res.SimilarID != similarID {
		t.Errorf("SimilarID = %v, want %v", res.SimilarID, similarID)
	}
	if res.Similarity != 0.95 {
		t.Errorf("Similarity = %f, want 0.95", res.Similarity)
	}
}

func TestCheck_Unique(t *testing.T) {
	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: uuid.New(), Score: 0.70, NamespaceID: uuid.New()},
		},
	}
	ep := &dedupMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	res, err := dedup.Check(context.Background(), "unique content", uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsDuplicate {
		t.Error("expected IsDuplicate=false for score 0.70 < threshold 0.92")
	}
	if res.Similarity != 0.70 {
		t.Errorf("Similarity = %f, want 0.70", res.Similarity)
	}
}

func TestCheck_NoEmbeddingProvider(t *testing.T) {
	vs := &mockVectorSearcher{}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, nil, DefaultDeduplicationConfig)

	res, err := dedup.Check(context.Background(), "anything", uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsDuplicate {
		t.Error("expected IsDuplicate=false when no embedding provider")
	}
	if res.SimilarID != nil {
		t.Error("expected SimilarID=nil when no embedding provider")
	}
}

func TestCheck_NoVectorResults(t *testing.T) {
	vs := &mockVectorSearcher{results: nil}
	ep := &dedupMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	res, err := dedup.Check(context.Background(), "novel content", uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsDuplicate {
		t.Error("expected IsDuplicate=false when no vector results")
	}
	if res.SimilarID != nil {
		t.Error("expected SimilarID=nil when no vector results")
	}
	if res.Similarity != 0 {
		t.Errorf("Similarity = %f, want 0", res.Similarity)
	}
}

func TestCheck_ExactThresholdBoundary(t *testing.T) {
	similarID := uuid.New()
	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: similarID, Score: 0.92, NamespaceID: uuid.New()},
		},
	}
	ep := &dedupMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	res, err := dedup.Check(context.Background(), "borderline content", uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsDuplicate {
		t.Error("expected IsDuplicate=true when score == threshold (0.92)")
	}
	if res.Similarity != 0.92 {
		t.Errorf("Similarity = %f, want 0.92", res.Similarity)
	}
}

func TestCheck_CustomThreshold(t *testing.T) {
	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: uuid.New(), Score: 0.85, NamespaceID: uuid.New()},
		},
	}
	ep := &dedupMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	// Use a lower threshold of 0.80.
	cfg := DeduplicationConfig{Threshold: 0.80, TopK: 3}
	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, cfg)

	res, err := dedup.Check(context.Background(), "some content", uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsDuplicate {
		t.Error("expected IsDuplicate=true for score 0.85 >= custom threshold 0.80")
	}
}

func TestCheck_EmptyContent(t *testing.T) {
	vs := &mockVectorSearcher{results: nil}
	ep := &dedupMockEmbedder{embeddings: [][]float32{{0.0, 0.0, 0.0}}}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	res, err := dedup.Check(context.Background(), "", uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsDuplicate {
		t.Error("expected IsDuplicate=false for empty content with no matches")
	}
}

func TestCheckBatch_MixedResults(t *testing.T) {
	dupID := uuid.New()
	uniqueID := uuid.New()
	nsID := uuid.New()

	callCount := 0
	vs := &mockVectorSearcherFunc{
		searchFn: func(_ context.Context, _ storage.VectorKind, emb []float32, _ uuid.UUID, _ int, _ int) ([]storage.VectorSearchResult, error) {
			defer func() { callCount++ }()
			switch callCount {
			case 0: // first content — duplicate
				return []storage.VectorSearchResult{
					{ID: dupID, Score: 0.96, NamespaceID: nsID},
				}, nil
			case 1: // second content — unique
				return []storage.VectorSearchResult{
					{ID: uniqueID, Score: 0.50, NamespaceID: nsID},
				}, nil
			case 2: // third content — no results
				return nil, nil
			default:
				return nil, nil
			}
		},
	}

	ep := &dedupMockEmbedder{
		embeddings: [][]float32{
			{0.1, 0.2, 0.3},
			{0.4, 0.5, 0.6},
			{0.7, 0.8, 0.9},
		},
	}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	results, err := dedup.CheckBatch(context.Background(), []string{"dup", "unique", "novel"}, nsID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First: duplicate.
	if !results[0].IsDuplicate {
		t.Error("results[0]: expected IsDuplicate=true")
	}
	if results[0].SimilarID == nil || *results[0].SimilarID != dupID {
		t.Errorf("results[0]: SimilarID = %v, want %v", results[0].SimilarID, dupID)
	}
	if results[0].Similarity != 0.96 {
		t.Errorf("results[0]: Similarity = %f, want 0.96", results[0].Similarity)
	}

	// Second: unique.
	if results[1].IsDuplicate {
		t.Error("results[1]: expected IsDuplicate=false")
	}
	if results[1].Similarity != 0.50 {
		t.Errorf("results[1]: Similarity = %f, want 0.50", results[1].Similarity)
	}

	// Third: no results.
	if results[2].IsDuplicate {
		t.Error("results[2]: expected IsDuplicate=false")
	}
	if results[2].SimilarID != nil {
		t.Error("results[2]: expected SimilarID=nil")
	}
	if results[2].Similarity != 0 {
		t.Errorf("results[2]: Similarity = %f, want 0", results[2].Similarity)
	}
}

func TestCheckBatch_NoEmbeddingProvider(t *testing.T) {
	vs := &mockVectorSearcher{}
	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, nil, DefaultDeduplicationConfig)

	results, err := dedup.CheckBatch(context.Background(), []string{"a", "b"}, uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for i, r := range results {
		if r.IsDuplicate {
			t.Errorf("results[%d]: expected IsDuplicate=false", i)
		}
	}
}

func TestCheckBatch_EmptyInput(t *testing.T) {
	vs := &mockVectorSearcher{}
	ep := &dedupMockEmbedder{}
	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	results, err := dedup.CheckBatch(context.Background(), nil, uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty input, got %v", results)
	}
}

func TestCheck_EmbedError(t *testing.T) {
	vs := &mockVectorSearcher{}
	ep := &dedupMockEmbedder{err: fmt.Errorf("embedding service down")}
	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	_, err := dedup.Check(context.Background(), "content", uuid.New())
	if err == nil {
		t.Fatal("expected error when embedding fails")
	}
}

func TestCheck_VectorSearchError(t *testing.T) {
	vs := &mockVectorSearcher{err: fmt.Errorf("vector store down")}
	ep := &dedupMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}
	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return ep }, nil, DefaultDeduplicationConfig)

	_, err := dedup.Check(context.Background(), "content", uuid.New())
	if err == nil {
		t.Fatal("expected error when vector search fails")
	}
}

func TestConfigWithDefaults(t *testing.T) {
	// Zero-value config should get defaults.
	cfg := DeduplicationConfig{}.withDefaults()
	if cfg.Threshold != 0.92 {
		t.Errorf("Threshold = %f, want 0.92", cfg.Threshold)
	}
	if cfg.TopK != 5 {
		t.Errorf("TopK = %d, want 5", cfg.TopK)
	}

	// Explicit values should be preserved.
	cfg2 := DeduplicationConfig{Threshold: 0.80, TopK: 10}.withDefaults()
	if cfg2.Threshold != 0.80 {
		t.Errorf("Threshold = %f, want 0.80", cfg2.Threshold)
	}
	if cfg2.TopK != 10 {
		t.Errorf("TopK = %d, want 10", cfg2.TopK)
	}
}

// ---------------------------------------------------------------------------
// Helper mock with function-based Search for per-call control
// ---------------------------------------------------------------------------

type mockVectorSearcherFunc struct {
	searchFn func(ctx context.Context, kind storage.VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]storage.VectorSearchResult, error)
}

func (m *mockVectorSearcherFunc) Search(ctx context.Context, kind storage.VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]storage.VectorSearchResult, error) {
	return m.searchFn(ctx, kind, embedding, namespaceID, dimension, topK)
}

// ---------------------------------------------------------------------------
// FindNearMatches tests
// ---------------------------------------------------------------------------

func TestFindNearMatches_NoResults(t *testing.T) {
	vs := &mockVectorSearcher{results: nil}
	reader := newMockMemoryReader()
	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, reader, DefaultDeduplicationConfig)

	matches, err := dedup.FindNearMatches(context.Background(), []float32{0.1, 0.2, 0.3}, uuid.New(), 5, 0.92, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected no matches, got %d", len(matches))
	}
}

func TestFindNearMatches_FiltersBelowThreshold(t *testing.T) {
	hi := uuid.New()
	mid := uuid.New()
	lo := uuid.New()
	nsID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: hi, Score: 0.97, NamespaceID: nsID},
			{ID: mid, Score: 0.93, NamespaceID: nsID},
			{ID: lo, Score: 0.85, NamespaceID: nsID}, // below 0.92 threshold
		},
	}
	reader := newMockMemoryReader()
	for _, id := range []uuid.UUID{hi, mid, lo} {
		reader.byID[id] = &model.Memory{ID: id, NamespaceID: nsID, Content: "c-" + id.String(), CreatedAt: time.Now().UTC()}
	}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, reader, DefaultDeduplicationConfig)

	matches, err := dedup.FindNearMatches(context.Background(), []float32{0.1}, nsID, 5, 0.92, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches above threshold, got %d", len(matches))
	}
	// Sorted descending by score.
	if matches[0].ID != hi || matches[1].ID != mid {
		t.Errorf("ordering wrong: got %v, %v", matches[0].ID, matches[1].ID)
	}
}

func TestFindNearMatches_ExcludeSelf(t *testing.T) {
	selfID := uuid.New()
	otherID := uuid.New()
	nsID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: selfID, Score: 1.0, NamespaceID: nsID},
			{ID: otherID, Score: 0.95, NamespaceID: nsID},
		},
	}
	reader := newMockMemoryReader()
	reader.byID[otherID] = &model.Memory{ID: otherID, NamespaceID: nsID, Content: "other"}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, reader, DefaultDeduplicationConfig)

	matches, err := dedup.FindNearMatches(context.Background(), []float32{0.1}, nsID, 5, 0.92, &selfID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match (self filtered), got %d", len(matches))
	}
	if matches[0].ID != otherID {
		t.Errorf("expected otherID, got %v", matches[0].ID)
	}
}

func TestFindNearMatches_HydratesContent(t *testing.T) {
	id := uuid.New()
	nsID := uuid.New()
	created := time.Date(2026, 4, 28, 10, 0, 0, 0, time.UTC)

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: id, Score: 0.95, NamespaceID: nsID},
		},
	}
	reader := newMockMemoryReader()
	reader.byID[id] = &model.Memory{ID: id, NamespaceID: nsID, Content: "hydrated content", CreatedAt: created}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, reader, DefaultDeduplicationConfig)

	matches, err := dedup.FindNearMatches(context.Background(), []float32{0.1}, nsID, 5, 0.92, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].Content != "hydrated content" {
		t.Errorf("Content = %q, want hydrated content", matches[0].Content)
	}
	if !matches[0].CreatedAt.Equal(created) {
		t.Errorf("CreatedAt = %v, want %v", matches[0].CreatedAt, created)
	}
}

func TestFindNearMatches_SkipsMissingMemoryRow(t *testing.T) {
	idGood := uuid.New()
	idMissing := uuid.New()
	nsID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: idMissing, Score: 0.99, NamespaceID: nsID}, // reader returns error
			{ID: idGood, Score: 0.95, NamespaceID: nsID},
		},
	}
	reader := newMockMemoryReader()
	reader.byID[idGood] = &model.Memory{ID: idGood, NamespaceID: nsID, Content: "good"}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, reader, DefaultDeduplicationConfig)

	matches, err := dedup.FindNearMatches(context.Background(), []float32{0.1}, nsID, 5, 0.92, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(matches) != 1 || matches[0].ID != idGood {
		t.Errorf("expected only idGood, got %+v", matches)
	}
}

func TestFindNearMatches_TopKLimit(t *testing.T) {
	nsID := uuid.New()
	results := make([]storage.VectorSearchResult, 10)
	reader := newMockMemoryReader()
	for i := range results {
		id := uuid.New()
		results[i] = storage.VectorSearchResult{ID: id, Score: 0.99 - float64(i)*0.001, NamespaceID: nsID}
		reader.byID[id] = &model.Memory{ID: id, NamespaceID: nsID, Content: fmt.Sprintf("c-%d", i)}
	}
	vs := &mockVectorSearcher{results: results}

	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, reader, DefaultDeduplicationConfig)

	matches, err := dedup.FindNearMatches(context.Background(), []float32{0.1}, nsID, 3, 0.92, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(matches) != 3 {
		t.Errorf("expected 3 matches (topK), got %d", len(matches))
	}
}

func TestFindNearMatches_EmptyEmbedding(t *testing.T) {
	vs := &mockVectorSearcher{}
	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, nil, DefaultDeduplicationConfig)

	matches, err := dedup.FindNearMatches(context.Background(), nil, uuid.New(), 5, 0.92, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matches != nil {
		t.Errorf("expected nil matches for empty embedding, got %v", matches)
	}
}

func TestFindNearMatches_NilMemoryReader(t *testing.T) {
	id := uuid.New()
	nsID := uuid.New()
	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{{ID: id, Score: 0.95, NamespaceID: nsID}},
	}
	dedup := NewDeduplicator(vs, func() provider.EmbeddingProvider { return nil }, nil, DefaultDeduplicationConfig)

	matches, err := dedup.FindNearMatches(context.Background(), []float32{0.1}, nsID, 5, 0.92, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match even with nil reader, got %d", len(matches))
	}
	if matches[0].Content != "" {
		t.Errorf("expected empty Content with nil reader, got %q", matches[0].Content)
	}
}
