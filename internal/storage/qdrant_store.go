package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"

	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// qdrantFacetNamespace seeds the deterministic UUIDv5 point IDs for topic facets
// (facet_id > 0). Facet 0 keeps the bare memory_id as its point ID so existing
// single-vector points remain valid without re-keying.
var qdrantFacetNamespace = uuid.MustParse("6f9b1d2e-3a4c-5d6e-7f80-91a2b3c4d5e6")

// facetPointID returns the Qdrant point ID for a memory's facet. Facet 0 is the
// memory's own UUID (back-compatible with pre-facet points); higher facets get a
// stable UUIDv5 derived from the memory ID and facet index.
func facetPointID(memoryID uuid.UUID, facet int) uuid.UUID {
	if facet == 0 {
		return memoryID
	}
	return uuid.NewSHA1(qdrantFacetNamespace, []byte(memoryID.String()+":"+strconv.Itoa(facet)))
}

// QdrantConfig holds the connection parameters for a Qdrant vector store.
// All fields are sourced from the runtime settings registry (qdrant.* keys);
// the struct is local to the storage package because it's the only consumer.
type QdrantConfig struct {
	Addr             string // gRPC address, e.g. "localhost:6334"
	APIKey           string // API key for authentication
	UseTLS           bool   // enable TLS for the gRPC connection
	PoolSize         uint   // gRPC connection count (0 → 3)
	KeepAliveTime    int    // seconds between keepalive pings (0 → 10s, -1 → disabled)
	KeepAliveTimeout uint   // seconds to wait for a keepalive ack (0 → 2s)
}

// qdrantMemoryCollections maps supported vector dimensions to their memory
// collection names.
var qdrantMemoryCollections = map[int]string{
	384:  "nram_vectors_384",
	512:  "nram_vectors_512",
	768:  "nram_vectors_768",
	1024: "nram_vectors_1024",
	1536: "nram_vectors_1536",
	3072: "nram_vectors_3072",
}

// qdrantEntityCollections is the parallel set for entity vectors.
var qdrantEntityCollections = map[int]string{
	384:  "nram_entity_vectors_384",
	512:  "nram_entity_vectors_512",
	768:  "nram_entity_vectors_768",
	1024: "nram_entity_vectors_1024",
	1536: "nram_entity_vectors_1536",
	3072: "nram_entity_vectors_3072",
}

// QdrantStore implements VectorStore using Qdrant via gRPC.
type QdrantStore struct {
	client *qdrant.Client
	// maxFacetsFn resolves enrichment.multi_vector.max_facets so the faceted
	// Search over-fetch tracks the configured cap (see overFetchFor). Nil until
	// SetMaxFacetsResolver is called; nil falls back to the over-fetch floor.
	maxFacetsFn func() int
	// gate drops the faceted Search over-fetch to 1x when the multi-vector
	// feature is off or the namespace/dimension has no topic facets. Nil
	// (unwired) means always over-fetch, the pre-gate behavior.
	gate *facetGate
}

// SetMaxFacetsResolver injects the resolver for enrichment.multi_vector.max_facets
// so the faceted Search candidate window scales with the configured facet cap
// instead of a fixed multiplier. Wired at boot; safe to leave unset in tests.
func (s *QdrantStore) SetMaxFacetsResolver(fn func() int) { s.maxFacetsFn = fn }

// SetFacetGate wires the multi-vector feature resolver and presence-cache TTL
// resolver used to drop the faceted over-fetch to 1x when there is no facet work
// to do. Wired at boot; safe to leave unset (nil gate over-fetches unconditionally).
func (s *QdrantStore) SetFacetGate(enabledFn func() bool, ttlFn func() time.Duration) {
	s.gate = newFacetGate(enabledFn, ttlFn)
}

// hasTopicFacets reports whether the collection holds any topic-facet
// (facet_id > 0) points for the namespace. A non-exact filtered count used by the
// facet gate to drop the over-fetch when there are no topic facets to collapse.
func (s *QdrantStore) hasTopicFacets(ctx context.Context, collection string, namespaceID uuid.UUID) (bool, error) {
	gt := 0.0
	exact := false
	count, err := s.client.Count(ctx, &qdrant.CountPoints{
		CollectionName: collection,
		Filter: &qdrant.Filter{
			Must: []*qdrant.Condition{
				qdrant.NewMatch("namespace_id", namespaceID.String()),
				qdrant.NewRange("facet_id", &qdrant.Range{Gt: &gt}),
			},
		},
		Exact: &exact,
	})
	if err != nil {
		return false, fmt.Errorf("qdrant: probe topic facets in %s: %w", collection, err)
	}
	return count > 0, nil
}

// Compile-time interface check.
var (
	_ VectorStore       = (*QdrantStore)(nil)
	_ FacetVectorStore  = (*QdrantStore)(nil)
	_ FacetCosineReader = (*QdrantStore)(nil)
)

// NewQdrantStore creates a new QdrantStore connected using the given configuration.
func NewQdrantStore(cfg QdrantConfig) (*QdrantStore, error) {
	host, port, err := parseQdrantAddr(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("qdrant: invalid address %q: %w", cfg.Addr, err)
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:             host,
		Port:             port,
		APIKey:           cfg.APIKey,
		UseTLS:           cfg.UseTLS,
		PoolSize:         cfg.PoolSize,
		KeepAliveTime:    cfg.KeepAliveTime,
		KeepAliveTimeout: cfg.KeepAliveTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("qdrant: failed to connect to %s: %w", cfg.Addr, err)
	}

	return &QdrantStore{client: client}, nil
}

// NewQdrantStoreFromClient creates a QdrantStore from an existing Qdrant client.
// Useful for testing with a pre-configured client.
func NewQdrantStoreFromClient(client *qdrant.Client) *QdrantStore {
	return &QdrantStore{client: client}
}

// parseQdrantAddr splits "host:port" into host string and port int.
func parseQdrantAddr(addr string) (string, int, error) {
	parts := strings.SplitN(addr, ":", 2)
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("expected host:port format")
	}
	host := parts[0]
	var port int
	if _, err := fmt.Sscanf(parts[1], "%d", &port); err != nil {
		return "", 0, fmt.Errorf("invalid port %q: %w", parts[1], err)
	}
	return host, port, nil
}

// qdrantCollectionName maps (Kind, dimension) to its Qdrant collection name.
func qdrantCollectionName(kind VectorKind, dimension int) (string, error) {
	switch kind {
	case "", VectorKindMemory:
		name, ok := qdrantMemoryCollections[dimension]
		if !ok {
			return "", fmt.Errorf("qdrant: unsupported memory dimension %d; supported: 384, 512, 768, 1024, 1536, 3072", dimension)
		}
		return name, nil
	case VectorKindEntity:
		name, ok := qdrantEntityCollections[dimension]
		if !ok {
			return "", fmt.Errorf("qdrant: unsupported entity dimension %d; supported: 384, 512, 768, 1024, 1536, 3072", dimension)
		}
		return name, nil
	default:
		return "", fmt.Errorf("qdrant: unknown vector kind %q", kind)
	}
}

// collectionsForKind returns the dim→collection map for the given kind.
func collectionsForKind(kind VectorKind) (map[int]string, error) {
	switch kind {
	case "", VectorKindMemory:
		return qdrantMemoryCollections, nil
	case VectorKindEntity:
		return qdrantEntityCollections, nil
	default:
		return nil, fmt.Errorf("qdrant: unknown vector kind %q", kind)
	}
}

// EnsureCollections creates all dimension-specific collections (memory + entity)
// if they do not already exist. Uses cosine distance metric. Should be called
// during server startup.
func (s *QdrantStore) EnsureCollections(ctx context.Context) error {
	if err := s.ensureCollectionFamily(ctx, qdrantMemoryCollections); err != nil {
		return err
	}
	return s.ensureCollectionFamily(ctx, qdrantEntityCollections)
}

func (s *QdrantStore) ensureCollectionFamily(ctx context.Context, family map[int]string) error {
	for dim, name := range family {
		exists, err := s.client.CollectionExists(ctx, name)
		if err != nil {
			return fmt.Errorf("qdrant: failed to check collection %s: %w", name, err)
		}
		if exists {
			continue
		}

		err = s.client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: name,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     uint64(dim),
				Distance: qdrant.Distance_Cosine,
			}),
		})
		if err != nil {
			return fmt.Errorf("qdrant: failed to create collection %s: %w", name, err)
		}

		// Create a keyword index on namespace_id for efficient filtering.
		_, err = s.client.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
			CollectionName: name,
			FieldName:      "namespace_id",
			FieldType:      qdrant.PtrOf(qdrant.FieldType_FieldTypeKeyword),
		})
		if err != nil {
			return fmt.Errorf("qdrant: failed to create namespace_id index on %s: %w", name, err)
		}
	}
	return nil
}

// Upsert inserts or updates a single vector in the appropriate dimension collection.
func (s *QdrantStore) Upsert(ctx context.Context, kind VectorKind, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error {
	collection, err := qdrantCollectionName(kind, dimension)
	if err != nil {
		return err
	}

	wait := true
	_, err = s.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection,
		Wait:           &wait,
		Points: []*qdrant.PointStruct{
			{
				Id:      qdrant.NewID(id.String()),
				Vectors: qdrant.NewVectorsDense(embedding),
				Payload: qdrant.NewValueMap(map[string]any{
					"namespace_id": namespaceID.String(),
				}),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("qdrant: upsert failed for collection %s: %w", collection, err)
	}
	return nil
}

// UpsertBatch inserts or updates multiple vectors, grouping by (kind, dimension)
// for efficiency.
func (s *QdrantStore) UpsertBatch(ctx context.Context, items []VectorUpsertItem) error {
	if len(items) == 0 {
		return nil
	}

	type batchKey struct {
		kind VectorKind
		dim  int
	}

	groups := make(map[batchKey][]VectorUpsertItem)
	for _, item := range items {
		k := item.EffectiveKind()
		if _, err := qdrantCollectionName(k, item.Dimension); err != nil {
			return err
		}
		key := batchKey{kind: k, dim: item.Dimension}
		groups[key] = append(groups[key], item)
	}

	for key, group := range groups {
		collection, _ := qdrantCollectionName(key.kind, key.dim) // already validated above

		points := make([]*qdrant.PointStruct, len(group))
		for i, item := range group {
			points[i] = &qdrant.PointStruct{
				Id:      qdrant.NewID(item.ID.String()),
				Vectors: qdrant.NewVectorsDense(item.Embedding),
				Payload: qdrant.NewValueMap(map[string]any{
					"namespace_id": item.NamespaceID.String(),
				}),
			}
		}

		wait := true
		_, err := s.client.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: collection,
			Wait:           &wait,
			Points:         points,
		})
		if err != nil {
			return fmt.Errorf("qdrant: batch upsert failed for collection %s: %w", collection, err)
		}
	}

	return nil
}

// IterateVectors scrolls every point in the (kind, dimension) collection and
// invokes fn with the point's UUID, its namespace_id payload, and a copy of the
// dense vector. It is read-only and used by the vector migrator to copy data
// out of Qdrant without exposing the gRPC client. A missing collection is
// treated as empty (fn is never called). Any point missing a parseable
// namespace_id payload or a dense vector is a hard error rather than a silent
// skip, so a corrupt copy cannot pass unnoticed.
func (s *QdrantStore) IterateVectors(ctx context.Context, kind VectorKind, dimension int, batchSize int, fn func(id, namespaceID uuid.UUID, vec []float32) error) error {
	collection, err := qdrantCollectionName(kind, dimension)
	if err != nil {
		return err
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	exists, err := s.client.CollectionExists(ctx, collection)
	if err != nil {
		return fmt.Errorf("qdrant: probe collection %s: %w", collection, err)
	}
	if !exists {
		return nil
	}

	limit := uint32(batchSize)
	var offset *qdrant.PointId
	for {
		points, err := s.client.Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: collection,
			Limit:          &limit,
			Offset:         offset,
			WithVectors:    qdrant.NewWithVectors(true),
			WithPayload:    qdrant.NewWithPayload(true),
		})
		if err != nil {
			return fmt.Errorf("qdrant: scroll %s failed: %w", collection, err)
		}
		if len(points) == 0 {
			break
		}
		for _, pt := range points {
			// Memory collections are faceted: facet 0 (and legacy single-vector
			// points) are keyed by memory_id, while topic facets (facet_id > 0)
			// use a derived point ID and are regenerated by the multi-vector
			// backfill after a backend switch. Copy only facet 0 so the migrator
			// never writes a topic facet back as a phantom facet-0 row, mirroring
			// the SQL->Qdrant forward path (streamPgVectors/streamSQLiteVectors,
			// which filter facet_id = 0). Absent payload (entities, pre-facet
			// points) reads as 0 and is kept.
			if pt.GetPayload()["facet_id"].GetIntegerValue() > 0 {
				continue
			}
			id, err := pointIDToUUID(pt.GetId())
			if err != nil {
				return fmt.Errorf("qdrant: invalid point ID in %s: %w", collection, err)
			}
			nsStr := pt.GetPayload()["namespace_id"].GetStringValue()
			nsID, err := uuid.Parse(nsStr)
			if err != nil {
				return fmt.Errorf("qdrant: point %s in %s has invalid namespace_id payload %q: %w", id, collection, nsStr, err)
			}
			vec := pt.GetVectors().GetVector().GetDenseVector().GetData()
			if len(vec) == 0 {
				return fmt.Errorf("qdrant: point %s in %s has no dense vector", id, collection)
			}
			cp := make([]float32, len(vec))
			copy(cp, vec)
			if err := fn(id, nsID, cp); err != nil {
				return err
			}
		}
		if len(points) < batchSize {
			break
		}
		offset = points[len(points)-1].GetId()
	}
	return nil
}

// CountVectors returns the exact number of points in the (kind, dimension)
// collection, or 0 if the collection does not exist. Used for migration
// verification so a partial copy is detectable.
func (s *QdrantStore) CountVectors(ctx context.Context, kind VectorKind, dimension int) (int, error) {
	collection, err := qdrantCollectionName(kind, dimension)
	if err != nil {
		return 0, err
	}
	exists, err := s.client.CollectionExists(ctx, collection)
	if err != nil {
		return 0, fmt.Errorf("qdrant: probe collection %s: %w", collection, err)
	}
	if !exists {
		return 0, nil
	}
	exact := true
	count, err := s.client.Count(ctx, &qdrant.CountPoints{
		CollectionName: collection,
		Exact:          &exact,
	})
	if err != nil {
		return 0, fmt.Errorf("qdrant: count %s failed: %w", collection, err)
	}
	return int(count), nil
}

// TotalMemoryVectors sums the memory-vector point counts across every supported
// dimension. Used by the startup activation guard to detect a Qdrant store that
// is active but empty.
func (s *QdrantStore) TotalMemoryVectors(ctx context.Context) (int, error) {
	total := 0
	for _, dim := range OrderedVectorDimensions {
		n, err := s.CountVectors(ctx, VectorKindMemory, dim)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// Search finds the nearest vectors within a namespace using cosine similarity.
// Filters by namespace_id payload field. The caller is responsible for soft-delete exclusion.
// UpsertFacets atomically replaces a memory's facet set at the given dimension.
// facets[0] is facet 0 (point ID = memory_id, back-compatible) and facets[1:]
// are topic facets with deterministic UUIDv5 point IDs. Every point carries a
// memory_id payload so Search can collapse and Delete can filter. An empty slice
// removes all facets for the memory.
func (s *QdrantStore) UpsertFacets(ctx context.Context, memoryID uuid.UUID, namespaceID uuid.UUID, dimension int, facets [][]float32) error {
	collection, err := qdrantCollectionName(VectorKindMemory, dimension)
	if err != nil {
		return err
	}
	if err := s.deleteAllFacets(ctx, collection, memoryID); err != nil {
		return err
	}
	// deleteAllFacets has already changed this memory's facet set, so every path
	// past here alters topic-facet presence for the namespace/dimension; drop the
	// cached presence answer once, covering both the empty-set and upsert returns.
	defer s.gate.invalidate(namespaceID, dimension)
	if len(facets) == 0 {
		return nil
	}
	points := make([]*qdrant.PointStruct, len(facets))
	for i, vec := range facets {
		points[i] = &qdrant.PointStruct{
			Id:      qdrant.NewID(facetPointID(memoryID, i).String()),
			Vectors: qdrant.NewVectorsDense(vec),
			Payload: qdrant.NewValueMap(map[string]any{
				"namespace_id": namespaceID.String(),
				"memory_id":    memoryID.String(),
				"facet_id":     i,
			}),
		}
	}
	wait := true
	if _, err := s.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection,
		Wait:           &wait,
		Points:         points,
	}); err != nil {
		return fmt.Errorf("qdrant: upsert-facets %s: %w", collection, err)
	}
	return nil
}

// deleteAllFacets removes both the facet-0 point (id = memory_id, including
// pre-facet points that lack a memory_id payload) and every topic-facet point
// (matched by the memory_id payload filter) for a memory in one collection.
func (s *QdrantStore) deleteAllFacets(ctx context.Context, collection string, memoryID uuid.UUID) error {
	wait := true
	if _, err := s.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: collection,
		Wait:           &wait,
		Points:         qdrant.NewPointsSelector(qdrant.NewID(memoryID.String())),
	}); err != nil {
		return fmt.Errorf("qdrant: delete facet-0 from %s: %w", collection, err)
	}
	if _, err := s.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: collection,
		Wait:           &wait,
		Points: qdrant.NewPointsSelectorFilter(&qdrant.Filter{
			Must: []*qdrant.Condition{qdrant.NewMatch("memory_id", memoryID.String())},
		}),
	}); err != nil {
		return fmt.Errorf("qdrant: delete topic facets from %s: %w", collection, err)
	}
	return nil
}

func (s *QdrantStore) Search(ctx context.Context, kind VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]VectorSearchResult, error) {
	collection, err := qdrantCollectionName(kind, dimension)
	if err != nil {
		return nil, err
	}

	// Memory points are faceted: over-fetch and collapse to the best facet per
	// memory. A topic-facet point carries its memory_id in the payload; a
	// facet-0 point (or any pre-facet point) does not, so its point ID is the
	// memory_id. Entity points are unaffected (no facets, no memory_id payload).
	faceted := isFacetedKind(kind)
	limit := uint64(topK)
	if faceted {
		of := effectiveOverFetch(ctx, s.gate, namespaceID, dimension, s.maxFacetsFn, func(pctx context.Context) (bool, error) {
			return s.hasTopicFacets(pctx, collection, namespaceID)
		})
		limit = uint64(topK * of)
	}
	scored, err := s.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          qdrant.NewQueryDense(embedding),
		Filter: &qdrant.Filter{
			Must: []*qdrant.Condition{
				qdrant.NewMatch("namespace_id", namespaceID.String()),
			},
		},
		Limit:       &limit,
		WithPayload: qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("qdrant: search query failed: %w", err)
	}

	best := make(map[uuid.UUID]float64, len(scored))
	for _, pt := range scored {
		mid, err := pointIDToUUID(pt.GetId())
		if err != nil {
			return nil, fmt.Errorf("qdrant: invalid point ID in search result: %w", err)
		}
		if mv, ok := pt.GetPayload()["memory_id"]; ok {
			if s := mv.GetStringValue(); s != "" {
				if parsed, perr := uuid.Parse(s); perr == nil {
					mid = parsed
				}
			}
		}
		score := float64(pt.GetScore())
		if cur, ok := best[mid]; !ok || score > cur {
			best[mid] = score
		}
	}

	return collapseFacets(best, namespaceID, topK), nil
}

func (s *QdrantStore) GetByIDs(ctx context.Context, kind VectorKind, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error) {
	if len(ids) == 0 {
		return map[uuid.UUID][]float32{}, nil
	}
	collection, err := qdrantCollectionName(kind, dimension)
	if err != nil {
		return nil, err
	}

	pointIDs := make([]*qdrant.PointId, len(ids))
	for i, id := range ids {
		pointIDs[i] = qdrant.NewID(id.String())
	}

	points, err := s.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: collection,
		Ids:            pointIDs,
		WithVectors:    qdrant.NewWithVectorsEnable(true),
		WithPayload:    qdrant.NewWithPayloadEnable(false),
	})
	if err != nil {
		return nil, fmt.Errorf("qdrant: get-by-ids failed for collection %s: %w", collection, err)
	}

	out := make(map[uuid.UUID][]float32, len(points))
	for _, pt := range points {
		id, err := pointIDToUUID(pt.GetId())
		if err != nil {
			return nil, fmt.Errorf("qdrant: invalid point ID in get-by-ids result: %w", err)
		}
		// Qdrant 1.13+ returns dense vectors in the typed Dense subfield. The
		// older flat Data field is only populated for the legacy single-vector
		// shape and stays empty when the point was upserted via
		// NewVectorsDense. Read Dense first; fall back to Data so older
		// servers keep working.
		v := pt.GetVectors().GetVector()
		vec := v.GetDenseVector().GetData()
		if len(vec) == 0 {
			// Sparse or named-vector points are not produced by this store;
			// skip rather than return a zero-length slice that callers would
			// have to special-case.
			continue
		}
		// Defensive copy so callers can't mutate proto-owned memory.
		cp := make([]float32, len(vec))
		copy(cp, vec)
		out[id] = cp
	}
	return out, nil
}

// BestFacetCosines folds each id's facets to its single best cosine against the
// query. Facets live as separate points (facet 0 keyed by memory_id, topic facets
// at deterministic facetPointID UUIDs), so it enumerates every candidate point id
// per memory up to the configured max_facets, fetches their vectors in one Get,
// maps each returned point back to its memory via the locally-built point->memory
// table (no payload dependency), and keeps the max cosine. Non-faceted kinds
// (entities) resolve to the single id-keyed point.
func (s *QdrantStore) BestFacetCosines(ctx context.Context, kind VectorKind, ids []uuid.UUID, query []float32, dimension int) (map[uuid.UUID]float64, error) {
	out := make(map[uuid.UUID]float64, len(ids))
	if len(ids) == 0 || len(query) == 0 {
		return out, nil
	}
	collection, err := qdrantCollectionName(kind, dimension)
	if err != nil {
		return nil, err
	}

	// Enumerate up to overFetchFor facets per memory: the same configured
	// max_facets (floored at facetSearchOverFetch) that sizes Search's candidate
	// window. Flooring rather than taking the raw config is deliberately
	// over-inclusive — a facet written when the cap was higher is still found, and
	// non-existent candidate points are simply absent from the Get.
	maxF := overFetchFor(s.maxFacetsFn)
	faceted := isFacetedKind(kind)

	// Candidate point ids per memory, with a reverse map so a returned point is
	// attributed without trusting payload (facet 0's point id IS the memory id;
	// topic facets are deterministic UUIDv5s we generate here).
	owner := make(map[uuid.UUID]uuid.UUID, len(ids)*maxF)
	pointIDs := make([]*qdrant.PointId, 0, len(ids)*maxF)
	for _, id := range ids {
		owner[id] = id
		pointIDs = append(pointIDs, qdrant.NewID(id.String()))
		if !faceted {
			continue
		}
		for f := 1; f <= maxF; f++ {
			pid := facetPointID(id, f)
			owner[pid] = id
			pointIDs = append(pointIDs, qdrant.NewID(pid.String()))
		}
	}

	points, err := s.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: collection,
		Ids:            pointIDs,
		WithVectors:    qdrant.NewWithVectorsEnable(true),
		WithPayload:    qdrant.NewWithPayloadEnable(false),
	})
	if err != nil {
		return nil, fmt.Errorf("qdrant: best-facet-cosines get failed for collection %s: %w", collection, err)
	}

	for _, pt := range points {
		pid, err := pointIDToUUID(pt.GetId())
		if err != nil {
			return nil, fmt.Errorf("qdrant: invalid point ID in best-facet-cosines result: %w", err)
		}
		mid, ok := owner[pid]
		if !ok {
			continue
		}
		v := pt.GetVectors().GetVector()
		vec := v.GetDenseVector().GetData()
		if len(vec) == 0 {
			continue
		}
		score := hnsw.CosineSimilarity(query, vec)
		if cur, ok := out[mid]; !ok || score > cur {
			out[mid] = score
		}
	}
	return out, nil
}

// Delete removes a vector from every dimension collection of the given kind,
// since the dimension is unknown at delete time. Does not error if the point
// does not exist in a collection.
func (s *QdrantStore) Delete(ctx context.Context, kind VectorKind, id uuid.UUID) error {
	collections, err := collectionsForKind(kind)
	if err != nil {
		return err
	}
	faceted := isFacetedKind(kind)

	for _, collection := range collections {
		if faceted {
			// Remove facet 0 (point id = memory_id) and every topic facet
			// (memory_id payload filter).
			if err := s.deleteAllFacets(ctx, collection, id); err != nil {
				return err
			}
			continue
		}
		if _, err := s.client.Delete(ctx, &qdrant.DeletePoints{
			CollectionName: collection,
			Points:         qdrant.NewPointsSelector(qdrant.NewID(id.String())),
		}); err != nil {
			return fmt.Errorf("qdrant: delete from %s failed: %w", collection, err)
		}
	}
	return nil
}

// Ping checks connectivity to the Qdrant server via a health check.
// TruncateAllVectors drops every collection and recreates via
// EnsureCollections. Drop-and-recreate is cheaper than walking every
// point and EnsureCollections re-applies the namespace_id payload index.
func (s *QdrantStore) TruncateAllVectors(ctx context.Context) error {
	for _, family := range []map[int]string{qdrantMemoryCollections, qdrantEntityCollections} {
		for _, name := range family {
			exists, err := s.client.CollectionExists(ctx, name)
			if err != nil {
				return fmt.Errorf("qdrant: probe collection %s: %w", name, err)
			}
			if !exists {
				continue
			}
			if err := s.client.DeleteCollection(ctx, name); err != nil {
				return fmt.Errorf("qdrant: delete collection %s: %w", name, err)
			}
		}
	}
	if err := s.EnsureCollections(ctx); err != nil {
		return fmt.Errorf("qdrant: recreate collections after truncate: %w", err)
	}
	s.gate.invalidateAll()
	return nil
}

func (s *QdrantStore) Ping(ctx context.Context) error {
	_, err := s.client.HealthCheck(ctx)
	if err != nil {
		return fmt.Errorf("qdrant: health check failed: %w", err)
	}
	return nil
}

// Client returns the underlying Qdrant client. Exported for test cleanup.
func (s *QdrantStore) Client() *qdrant.Client {
	return s.client
}

// Close tears down the gRPC connection to Qdrant.
func (s *QdrantStore) Close() error {
	return s.client.Close()
}

// pointIDToUUID extracts a UUID from a Qdrant PointId.
func pointIDToUUID(pid *qdrant.PointId) (uuid.UUID, error) {
	if pid == nil {
		return uuid.Nil, fmt.Errorf("qdrant: nil point ID")
	}
	uuidOpt, ok := pid.GetPointIdOptions().(*qdrant.PointId_Uuid)
	if !ok {
		return uuid.Nil, fmt.Errorf("qdrant: point ID is not a UUID")
	}
	return uuid.Parse(uuidOpt.Uuid)
}
