package metrics

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/storage"
)

// fakeVectorStore implements storage.VectorStore only (no facet capability), so
// it stands in for a backend that does not support multi-vector facets.
type fakeVectorStore struct{}

func (f *fakeVectorStore) Upsert(context.Context, storage.VectorKind, uuid.UUID, uuid.UUID, []float32, int) error {
	return nil
}
func (f *fakeVectorStore) UpsertBatch(context.Context, []storage.VectorUpsertItem) error { return nil }
func (f *fakeVectorStore) Search(context.Context, storage.VectorKind, []float32, uuid.UUID, int, int) ([]storage.VectorSearchResult, error) {
	return nil, nil
}
func (f *fakeVectorStore) GetByIDs(context.Context, storage.VectorKind, []uuid.UUID, int) (map[uuid.UUID][]float32, error) {
	return nil, nil
}
func (f *fakeVectorStore) Delete(context.Context, storage.VectorKind, uuid.UUID) error { return nil }
func (f *fakeVectorStore) TruncateAllVectors(context.Context) error                    { return nil }
func (f *fakeVectorStore) Ping(context.Context) error                                  { return nil }

// fakeFacetVectorStore adds the facet capability and records the last UpsertFacets
// call so the test can prove the wrapper delegates rather than dropping the write.
type fakeFacetVectorStore struct {
	fakeVectorStore
	calls         int
	lastMemoryID  uuid.UUID
	lastFacetsLen int
}

func (f *fakeFacetVectorStore) UpsertFacets(_ context.Context, memoryID uuid.UUID, _ uuid.UUID, _ int, facets [][]float32) error {
	f.calls++
	f.lastMemoryID = memoryID
	f.lastFacetsLen = len(facets)
	return nil
}

// TestWrapVectorStore_PreservesFacetCapability guards the regression where the
// metrics decorator hid storage.FacetVectorStore from the enrichment worker, so
// the worker's type assertion failed and topic facets were silently never
// written on any backend. The wrapped store must still satisfy
// FacetVectorStore and forward the call to the underlying facet-capable store.
func TestWrapVectorStore_PreservesFacetCapability(t *testing.T) {
	inner := &fakeFacetVectorStore{}
	wrapped := WrapVectorStore(inner, New())

	fs, ok := wrapped.(storage.FacetVectorStore)
	if !ok {
		t.Fatal("wrapped vector store does not satisfy storage.FacetVectorStore; the worker assertion would fail and facets would never be written")
	}

	memID := uuid.New()
	facets := [][]float32{{1, 0}, {0, 1}, {0.5, 0.5}}
	if err := fs.UpsertFacets(context.Background(), memID, uuid.New(), 2, facets); err != nil {
		t.Fatalf("UpsertFacets through wrapper: %v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("expected UpsertFacets to delegate once to the inner store, got %d calls", inner.calls)
	}
	if inner.lastMemoryID != memID || inner.lastFacetsLen != len(facets) {
		t.Fatalf("wrapper passed wrong args: memID=%v facets=%d", inner.lastMemoryID, inner.lastFacetsLen)
	}
}

// TestWrapVectorStore_FacetOnNonFacetStoreErrors confirms that wrapping a store
// without facet support surfaces a clear error rather than silently dropping
// the facet write (the wrapper always advertises the capability for a stable
// type, so it must fail loudly when the underlying store cannot honour it).
func TestWrapVectorStore_FacetOnNonFacetStoreErrors(t *testing.T) {
	wrapped := WrapVectorStore(&fakeVectorStore{}, New())
	fs, ok := wrapped.(storage.FacetVectorStore)
	if !ok {
		t.Fatal("wrapped store should statically advertise FacetVectorStore")
	}
	if err := fs.UpsertFacets(context.Background(), uuid.New(), uuid.New(), 2, [][]float32{{1, 0}}); err == nil {
		t.Fatal("expected an error when the underlying store does not support facets, got nil")
	}
}
