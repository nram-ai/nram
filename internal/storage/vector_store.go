package storage

import (
	"context"

	"github.com/google/uuid"
)

// VectorSearchResult holds a single result from a vector similarity search.
type VectorSearchResult struct {
	ID          uuid.UUID `json:"id"`
	Score       float64   `json:"score"`
	NamespaceID uuid.UUID `json:"namespace_id"`
}

// VectorUpsertItem represents a single vector to upsert in a batch operation.
type VectorUpsertItem struct {
	ID          uuid.UUID `json:"id"`
	NamespaceID uuid.UUID `json:"namespace_id"`
	Embedding   []float32 `json:"embedding"`
	Dimension   int       `json:"dimension"`
}

// SupportedVectorDimensions is the set of embedding dimensions that the vector
// storage backends support. Both pgvector and Qdrant use this same set.
var SupportedVectorDimensions = map[int]bool{
	384:  true,
	512:  true,
	768:  true,
	1024: true,
	1536: true,
	3072: true,
}

// BestEmbeddingDimension picks the largest dimension that is supported by both
// the embedding provider and the vector store. Returns 0 if none of the
// provider's dimensions are supported.
func BestEmbeddingDimension(providerDims []int) int {
	best := 0
	for _, d := range providerDims {
		if SupportedVectorDimensions[d] && d > best {
			best = d
		}
	}
	return best
}

// VectorStore abstracts vector storage backends (pgvector, SQLite brute-force, Qdrant).
type VectorStore interface {
	// Upsert inserts or updates a single vector associated with a memory.
	Upsert(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error

	// UpsertBatch inserts or updates multiple vectors in a single operation.
	UpsertBatch(ctx context.Context, items []VectorUpsertItem) error

	// Search finds the nearest neighbor vectors within a namespace, returning up to topK results.
	Search(ctx context.Context, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]VectorSearchResult, error)

	// GetByIDs returns the stored embeddings for the given memory IDs at the
	// specified dimension. Missing IDs (no stored vector at this dimension)
	// are simply absent from the returned map; this is not an error. Callers
	// treat the absence as a miss and re-embed at the current dim. Vectors
	// stored at other dimensions are also absent — there is no cross-dim
	// retrieval, so a provider switch self-heals on the next pass that runs
	// at the new dim.
	GetByIDs(ctx context.Context, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error)

	// Delete removes a vector by its associated memory ID.
	Delete(ctx context.Context, id uuid.UUID) error

	// Ping checks vector store connectivity.
	Ping(ctx context.Context) error
}
