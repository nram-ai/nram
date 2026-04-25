package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/qdrant/go-client/qdrant"
)

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
}

// Compile-time interface check.
var _ VectorStore = (*QdrantStore)(nil)

// NewQdrantStore creates a new QdrantStore connected using the given configuration.
func NewQdrantStore(cfg config.QdrantConfig) (*QdrantStore, error) {
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

	_, err = s.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection,
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

		_, err := s.client.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: collection,
			Points:         points,
		})
		if err != nil {
			return fmt.Errorf("qdrant: batch upsert failed for collection %s: %w", collection, err)
		}
	}

	return nil
}

// Search finds the nearest vectors within a namespace using cosine similarity.
// Filters by namespace_id payload field. The caller is responsible for soft-delete exclusion.
func (s *QdrantStore) Search(ctx context.Context, kind VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]VectorSearchResult, error) {
	collection, err := qdrantCollectionName(kind, dimension)
	if err != nil {
		return nil, err
	}

	limit := uint64(topK)
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

	results := make([]VectorSearchResult, 0, len(scored))
	for _, pt := range scored {
		pointUUID, err := pointIDToUUID(pt.GetId())
		if err != nil {
			return nil, fmt.Errorf("qdrant: invalid point ID in search result: %w", err)
		}
		results = append(results, VectorSearchResult{
			ID:          pointUUID,
			Score:       float64(pt.GetScore()),
			NamespaceID: namespaceID,
		})
	}

	return results, nil
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
		vec := pt.GetVectors().GetVector().GetData()
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

// Delete removes a vector from every dimension collection of the given kind,
// since the dimension is unknown at delete time. Does not error if the point
// does not exist in a collection.
func (s *QdrantStore) Delete(ctx context.Context, kind VectorKind, id uuid.UUID) error {
	collections, err := collectionsForKind(kind)
	if err != nil {
		return err
	}
	pointID := qdrant.NewID(id.String())

	for _, collection := range collections {
		_, err := s.client.Delete(ctx, &qdrant.DeletePoints{
			CollectionName: collection,
			Points:         qdrant.NewPointsSelector(pointID),
		})
		if err != nil {
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
