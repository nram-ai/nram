package metrics

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/storage"
)

// instrumentedVectorStore wraps storage.VectorStore so every Search call
// records nram_vector_search_duration_seconds. Failed searches are still
// observed; the histogram captures latency for both outcomes, which is
// what an operator wants when diagnosing a slow backend. Upsert, Get,
// Delete, Truncate, and Ping are not instrumented at the storage layer:
// vector writes are already covered by the embedding metric upstream, and
// fast-path reads/deletes are not part of the recall hot path.
type instrumentedVectorStore struct {
	inner   storage.VectorStore
	metrics *Metrics
}

// WrapVectorStore returns vs wrapped with vector-search metrics. Returns vs
// unchanged when m is nil so callers can wire metrics conditionally.
func WrapVectorStore(vs storage.VectorStore, m *Metrics) storage.VectorStore {
	if vs == nil || m == nil {
		return vs
	}
	return &instrumentedVectorStore{inner: vs, metrics: m}
}

func (s *instrumentedVectorStore) Upsert(ctx context.Context, kind storage.VectorKind, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error {
	return s.inner.Upsert(ctx, kind, id, namespaceID, embedding, dimension)
}

func (s *instrumentedVectorStore) UpsertBatch(ctx context.Context, items []storage.VectorUpsertItem) error {
	return s.inner.UpsertBatch(ctx, items)
}

func (s *instrumentedVectorStore) Search(ctx context.Context, kind storage.VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]storage.VectorSearchResult, error) {
	start := time.Now()
	results, err := s.inner.Search(ctx, kind, embedding, namespaceID, dimension, topK)
	s.metrics.VectorSearchDuration.Observe(time.Since(start).Seconds())
	return results, err
}

func (s *instrumentedVectorStore) GetByIDs(ctx context.Context, kind storage.VectorKind, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error) {
	return s.inner.GetByIDs(ctx, kind, ids, dimension)
}

func (s *instrumentedVectorStore) Delete(ctx context.Context, kind storage.VectorKind, id uuid.UUID) error {
	return s.inner.Delete(ctx, kind, id)
}

func (s *instrumentedVectorStore) TruncateAllVectors(ctx context.Context) error {
	return s.inner.TruncateAllVectors(ctx)
}

func (s *instrumentedVectorStore) Ping(ctx context.Context) error {
	return s.inner.Ping(ctx)
}
