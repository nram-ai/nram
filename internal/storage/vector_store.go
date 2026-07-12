package storage

import (
	"context"
	"sort"

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
	// dimensions are also absent; there is no cross-dim retrieval, so a
	// provider switch self-heals on the next pass that runs at the new dim.
	GetByIDs(ctx context.Context, kind VectorKind, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error)

	// Delete removes a vector by its associated parent ID. Kind selects the
	// memory or entity table family.
	Delete(ctx context.Context, kind VectorKind, id uuid.UUID) error

	// TruncateAllVectors removes every vector across every dim and kind.
	// Schema is preserved. Used by the embedding-model switch cascade.
	TruncateAllVectors(ctx context.Context) error

	// Ping checks vector store connectivity.
	Ping(ctx context.Context) error
}

// FacetVectorStore is an optional capability implemented by vector backends that
// support multi-vector (per-facet) storage for memories. A memory's vector set
// is facet 0 (the pooled whole-memory embedding) plus zero or more topic
// facets. Backends that do not implement this interface degrade cleanly: a
// caller type-asserts and falls back to the base single-vector Upsert (facet 0
// only) when the assertion fails.
//
// Faceting applies to VectorKindMemory only; entity vectors are single-concept
// and stay single-vector. The read-side max-over-facets collapse is internal to
// each backend's Search, so Search keeps returning one result per memory (score
// = best facet) and no caller of Search changes.
type FacetVectorStore interface {
	// UpsertFacets atomically replaces the entire facet set for a memory at the
	// given dimension. facets[0] is facet 0 (the pooled whole-memory vector) and
	// facets[1:] are topic facets, written at facet_id = their slice index. An
	// empty slice removes every facet for the memory at that dimension.
	UpsertFacets(ctx context.Context, memoryID uuid.UUID, namespaceID uuid.UUID, dimension int, facets [][]float32) error
}

// FacetCosineReader is the read-side counterpart of FacetVectorStore: the
// optional capability to score known ids against a query on the best-facet scale.
// It is split from FacetVectorStore (the write capability) on purpose — a backend
// or test double may implement one without the other, and bundling them would
// make a fake that only writes facets silently fail the write-path type assertion.
type FacetCosineReader interface {
	// BestFacetCosines returns, per requested id, the maximum cosine similarity
	// between query and any of that id's stored facets (facet 0, the pooled
	// whole-memory vector, plus the topic facets). It is the by-id analogue of the
	// max-over-facets collapse that Search performs while ranking: a caller that
	// already knows the ids it cares about (rather than searching for them) gets a
	// cosine on the same best-facet scale a Search hit would carry. Missing ids
	// are absent from the map, like GetByIDs; this is not an error. Non-faceted
	// kinds (entities) collapse to their single vector. A nil/empty query returns
	// an empty map.
	BestFacetCosines(ctx context.Context, kind VectorKind, ids []uuid.UUID, query []float32, dimension int) (map[uuid.UUID]float64, error)
}

// collapseFacets turns a best-score-per-memory map (the result of folding a
// faceted Search's over-fetched candidates) into a deterministically ordered
// VectorSearchResult slice truncated to topK: score descending, ID ascending as
// a stable tiebreak. Shared by the in-memory-fold backends (HNSW, Qdrant) so the
// collapse ordering is provably identical; pgvector does the equivalent in SQL.
func collapseFacets(best map[uuid.UUID]float64, namespaceID uuid.UUID, topK int) []VectorSearchResult {
	out := make([]VectorSearchResult, 0, len(best))
	for id, score := range best {
		out = append(out, VectorSearchResult{ID: id, Score: score, NamespaceID: namespaceID})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID.String() < out[j].ID.String()
	})
	if len(out) > topK {
		out = out[:topK]
	}
	return out
}

// isFacetedKind reports whether a vector kind carries facets. Memory vectors are
// faceted; entity vectors are single-concept. The zero value defaults to memory,
// matching VectorUpsertItem.EffectiveKind. Single source of truth for the
// empty-string-means-memory rule that the SQL backends bake into their specs.
func isFacetedKind(kind VectorKind) bool {
	return kind == "" || kind == VectorKindMemory
}

// facetSearchOverFetch is the floor multiplier on topK when a faceted backend
// scans candidate facet rows, so that after collapsing multiple facets of the
// same memory to one result the query still yields topK distinct memories.
// It is a floor only: the effective multiplier tracks the configured
// enrichment.multi_vector.max_facets (see overFetchFor) because a memory can
// carry up to that many facets, and the candidate window must be at least
// max_facets * topK rows to guarantee topK distinct memories after collapse.
const facetSearchOverFetch = 8

// MaxFacetsUpperBound is the registered maximum of the
// enrichment.multi_vector.max_facets setting; the settings registration
// (internal/storage/admin/settings_store.go) references this constant for its Max
// bound, so the two cannot silently diverge. It is the most facets any single
// memory can ever carry.
const MaxFacetsUpperBound = 32

// facetOverFetchCeiling caps the retry-grown over-fetch multiplier for the
// in-band faceted backends (pgvector, Qdrant). It equals MaxFacetsUpperBound
// because a window of topK*facetOverFetchCeiling is then guaranteed to yield topK
// distinct memories after the best-facet collapse even if the max_facets setting
// was later lowered below the stored facet count. See effectiveOverFetch and
// retryGrowingOverFetch.
const facetOverFetchCeiling = MaxFacetsUpperBound

// overFetchFor returns the candidate over-fetch multiplier for a faceted Search.
// It is max(facetSearchOverFetch, configured max_facets): the window must hold at
// least max_facets * topK rows so that even if the top topK-1 memories each
// contribute their full max_facets facets, at least one more distinct memory
// remains to reach topK after the max-over-facets collapse. maxFacetsFn is the
// store's resolver for enrichment.multi_vector.max_facets; nil (test wiring or a
// store without the resolver attached) falls back to the floor.
func overFetchFor(maxFacetsFn func() int) int {
	of := facetSearchOverFetch
	if maxFacetsFn != nil {
		if mf := maxFacetsFn(); mf > of {
			of = mf
		}
	}
	return of
}

// effectiveOverFetch returns the starting candidate over-fetch multiplier for an
// in-band faceted Search (pgvector, Qdrant) and whether the search should be
// restricted to whole-memory (facet 0) rows. It folds the two inactive reasons
// the gate reports into distinct behavior:
//
//   - Feature disabled: return (1, true). Recall behaves as whole-memory-only,
//     matching HNSW (which skips its topic scan when disabled). The caller adds a
//     facet-0-only filter, so exactly one row per memory is scanned and the 1x
//     window is provably safe even though topic-facet rows still physically exist.
//   - No topic facets present (or probe error → fail-safe active): return the
//     over-fetch floor with wholeMemoryOnly=false. With zero topic facets the
//     collapse is a no-op; when active, the caller's Search retry loop grows the
//     window up to facetOverFetchCeiling if the first pass under-fills (which
//     covers a max_facets setting lowered below the stored facet count).
//
// probe is the backend's native topic-facet presence probe.
func effectiveOverFetch(ctx context.Context, gate *facetGate, namespaceID uuid.UUID, dimension int, maxFacetsFn func() int, probe func(context.Context) (bool, error)) (startOverFetch int, wholeMemoryOnly bool) {
	if !gate.featureEnabled() {
		return 1, true
	}
	if !gate.active(ctx, namespaceID, dimension, probe) {
		return 1, false
	}
	return overFetchFor(maxFacetsFn), false
}

// grownOverFetch returns the next over-fetch multiplier for a Search retry that
// under-filled: double it, capped at facetOverFetchCeiling. Returns of unchanged
// once the ceiling is reached so the caller can detect it cannot grow further.
func grownOverFetch(of int) int {
	return min(of*2, facetOverFetchCeiling)
}

// retryGrowingOverFetch drives the in-band faceted Search retry (pgvector,
// Qdrant): it runs the backend's query at a growing over-fetch multiplier until
// enough distinct memories collapse out, the candidate window is no longer
// saturated (true scarcity, so more candidates cannot exist), or the ceiling is
// reached. run executes one query at the given over-fetch and returns the
// backend's result, the number of distinct memories it collapsed to, and the raw
// candidate count the query returned (which equals topK*overFetch when the window
// is saturated). Centralizing the loop and this three-part termination test keeps
// the two backends from silently diverging on the subtle saturated-vs-scarce
// distinction.
func retryGrowingOverFetch[R any](topK, startOverFetch int, run func(overFetch int) (out R, distinct, rawCount int, err error)) (R, error) {
	of := startOverFetch
	for {
		out, distinct, rawCount, err := run(of)
		if err != nil {
			return out, err
		}
		if distinct >= topK || rawCount < topK*of || of >= facetOverFetchCeiling {
			return out, nil
		}
		of = grownOverFetch(of)
	}
}
