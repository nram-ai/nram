package storage

import (
	"context"

	"github.com/google/uuid"
)

// VectorKind disambiguates which entity family a vector belongs to. Routing
// is required because memory vectors and entity vectors live in separate
// tables/collections and reference different parent rows for namespace scoping.
type VectorKind string

const (
	VectorKindMemory VectorKind = "memory"
	VectorKindEntity VectorKind = "entity"
)

// VectorSearchResult holds a single result from a vector similarity search.
type VectorSearchResult struct {
	ID          uuid.UUID `json:"id"`
	Score       float64   `json:"score"`
	NamespaceID uuid.UUID `json:"namespace_id"`
}

// VectorUpsertItem represents a single vector to upsert in a batch operation.
// Kind selects the table family; zero value defaults to VectorKindMemory.
type VectorUpsertItem struct {
	Kind        VectorKind `json:"kind,omitempty"`
	ID          uuid.UUID  `json:"id"`
	NamespaceID uuid.UUID  `json:"namespace_id"`
	Embedding   []float32  `json:"embedding"`
	Dimension   int        `json:"dimension"`
}

// EffectiveKind returns the item's Kind, defaulting to VectorKindMemory when
// the field is the zero value.
func (i VectorUpsertItem) EffectiveKind() VectorKind {
	if i.Kind == "" {
		return VectorKindMemory
	}
	return i.Kind
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
	// Upsert inserts or updates a single vector. Kind selects the memory or
	// entity table family.
	Upsert(ctx context.Context, kind VectorKind, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error

	// UpsertBatch inserts or updates multiple vectors in a single operation.
	// Each item carries its own Kind; mixed-Kind batches are supported.
	UpsertBatch(ctx context.Context, items []VectorUpsertItem) error

	// Search finds the nearest neighbor vectors within a namespace, returning
	// up to topK results. Kind selects the memory or entity table family.
	Search(ctx context.Context, kind VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]VectorSearchResult, error)

	// GetByIDs returns the stored embeddings for the given IDs at the
	// specified dimension. Kind selects the memory or entity table family.
	// Missing IDs (no stored vector at this dimension) are simply absent
	// from the returned map; this is not an error. Callers treat the absence
	// as a miss and re-embed at the current dim. Vectors stored at other
	// dimensions are also absent — there is no cross-dim retrieval, so a
	// provider switch self-heals on the next pass that runs at the new dim.
	GetByIDs(ctx context.Context, kind VectorKind, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error)

	// Delete removes a vector by its associated parent ID. Kind selects the
	// memory or entity table family.
	Delete(ctx context.Context, kind VectorKind, id uuid.UUID) error

	// Ping checks vector store connectivity.
	Ping(ctx context.Context) error
}
