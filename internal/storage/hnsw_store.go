package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// isForeignKeyViolation reports whether err is a SQLite or Postgres foreign
// key constraint failure. Used by UpsertBatch to skip individual items whose
// parent row was deleted concurrently (e.g., lifecycle orphan-cleanup racing
// in-flight enrichment) instead of rolling the whole batch back and starving
// good rows. Driver-agnostic: string-matches the descriptive constraint
// phrases that both modernc.org/sqlite and pgx emit. We deliberately do
// not match the bare numeric code (e.g. "(787)") because that substring
// could appear in unrelated wrapped error text (row ids, offsets, byte
// counts) and false-positive into silent error absorption.
func isForeignKeyViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// modernc.org/sqlite formats this as "constraint failed: FOREIGN KEY
	// constraint failed".
	if strings.Contains(msg, "FOREIGN KEY constraint failed") {
		return true
	}
	// Postgres / pgx formats: "ERROR: ... violates foreign key constraint ..."
	// or carries SQLSTATE 23503.
	if strings.Contains(msg, "violates foreign key constraint") ||
		strings.Contains(msg, "SQLSTATE 23503") {
		return true
	}
	return false
}

// isUniqueViolation reports whether err is a SQLite or Postgres unique
// constraint failure. Companion to isForeignKeyViolation for the
// RelationshipRepo batch writers: a chunk-level violation triggers a
// rollback-to-savepoint and per-row retry so other rows still commit.
// As with isForeignKeyViolation, we match descriptive constraint phrases
// rather than the bare numeric code so unrelated text containing the
// number cannot false-positive.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "UNIQUE constraint failed") {
		return true
	}
	if strings.Contains(msg, "duplicate key value violates unique constraint") ||
		strings.Contains(msg, "SQLSTATE 23505") {
		return true
	}
	return false
}

// isTolerableRowError classifies row-level constraint errors that batch
// writers can absorb and continue past. Everything else (driver error,
// network, tx state) propagates and aborts the outer transaction.
func isTolerableRowError(err error) bool {
	return isForeignKeyViolation(err) || isUniqueViolation(err) || isOnConflictCardinalityError(err)
}

// isOnConflictCardinalityError detects the Postgres cardinality violation raised
// when one INSERT ... ON CONFLICT DO UPDATE statement carries two VALUES rows
// that map to the same conflict key (SQLSTATE 21000, "cannot affect row a
// second time"); e.g. two relation-formatting variants that canonicalize to
// the same (namespace_id, source_id, target_id, relation, valid_from). Treating
// it as tolerable lets the batch fall back to per-row inserts, where the second
// row conflicts with the first (now in the table) and merges via ON CONFLICT.
// SQLite never raises this; it applies each row's ON CONFLICT in statement
// order, so this is a Postgres-only recovery path.
func isOnConflictCardinalityError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "cannot affect row a second time") ||
		strings.Contains(msg, "SQLSTATE 21000")
}

// HNSWConfig holds configuration for the HNSW vector store.
type HNSWConfig struct {
	M                int
	EfConstruction   int
	EfSearch         int
	MaxLoadedIndexes int
	SnapshotInterval time.Duration
}

// DefaultHNSWConfig returns sensible defaults for HNSW configuration.
func DefaultHNSWConfig() HNSWConfig {
	return HNSWConfig{
		M:                16,
		EfConstruction:   200,
		EfSearch:         50,
		MaxLoadedIndexes: 64,
		SnapshotInterval: 60 * time.Second,
	}
}

// HNSWStore implements VectorStore using a pure-Go HNSW index backed by SQLite.
type HNSWStore struct {
	readDB  *sql.DB
	writeDB *sql.DB
	cache   *hnsw.IndexCache
	// gate short-circuits the per-recall topic-facet brute-force scan when the
	// multi-vector feature is off or the namespace/dimension has no topic facets.
	// Nil (unwired, e.g. tests or the migrate-back store) means always scan.
	gate *facetGate
	// scanCount counts how many times scanFacetCandidates ran its row loop.
	// Test-only observability for the facet-scan short-circuit (see ScanCount).
	scanCount atomic.Int64
}

// SetFacetGate wires the multi-vector feature resolver and presence-cache TTL
// resolver used to skip the per-recall topic-facet scan. Wired at boot; safe to
// leave unset (nil gate scans unconditionally, the pre-gate behavior).
func (s *HNSWStore) SetFacetGate(enabledFn func() bool, ttlFn func() time.Duration) {
	s.gate = newFacetGate(enabledFn, ttlFn)
}

// Compile-time interface check.
var (
	_ VectorStore       = (*HNSWStore)(nil)
	_ FacetVectorStore  = (*HNSWStore)(nil)
	_ FacetCosineReader = (*HNSWStore)(nil)
)

// NewHNSWStore creates a new HNSWStore backed by the given SQLite database.
// readDB is used for loading vectors; writeDB is used for upserts, deletes,
// and snapshot persistence.
func NewHNSWStore(readDB, writeDB *sql.DB, cfg HNSWConfig) *HNSWStore {
	// Apply defaults for zero-valued config fields.
	if cfg.M <= 0 {
		cfg.M = 16
	}
	if cfg.EfConstruction <= 0 {
		cfg.EfConstruction = 200
	}
	if cfg.EfSearch <= 0 {
		cfg.EfSearch = 50
	}
	if cfg.MaxLoadedIndexes <= 0 {
		cfg.MaxLoadedIndexes = 64
	}
	if cfg.SnapshotInterval <= 0 {
		cfg.SnapshotInterval = 60 * time.Second
	}

	cacheCfg := hnsw.CacheConfig{
		MaxIndexes:       cfg.MaxLoadedIndexes,
		SnapshotInterval: cfg.SnapshotInterval,
		GraphOpts: []hnsw.Option{
			hnsw.WithM(cfg.M),
			hnsw.WithEfConstruction(cfg.EfConstruction),
			hnsw.WithEfSearch(cfg.EfSearch),
		},
	}

	return &HNSWStore{
		readDB:  readDB,
		writeDB: writeDB,
		cache:   hnsw.NewIndexCache(readDB, writeDB, cacheCfg),
	}
}

type hnswTableSpec struct {
	cacheKind     hnsw.Kind
	vectorTable   string
	snapshotTable string
	idColumn      string
	// faceted is true for the memory table, which carries facet_id: facet 0 is
	// the pooled whole-memory vector (indexed in the graph) and facets 1..N are
	// topic facets (stored in SQLite, brute-forced at search time).
	faceted bool
}

var hnswSpecs = map[VectorKind]hnswTableSpec{
	VectorKindMemory: {cacheKind: hnsw.KindMemory, vectorTable: "memory_vectors", snapshotTable: "hnsw_snapshots", idColumn: "memory_id", faceted: true},
	VectorKindEntity: {cacheKind: hnsw.KindEntity, vectorTable: "entity_vectors", snapshotTable: "entity_hnsw_snapshots", idColumn: "entity_id"},
}

// hnswUpsertSQL builds the single-vector upsert. For the faceted memory table it
// targets facet 0 with the composite-key conflict so it never clobbers topic
// facets; entity rows conflict on the bare id column.
func hnswUpsertSQL(spec hnswTableSpec) string {
	if spec.faceted {
		return fmt.Sprintf(`
			INSERT INTO %s (%s, facet_id, namespace_id, dimension, embedding, created_at, updated_at)
			VALUES (?, 0, ?, ?, ?, ?, ?)
			ON CONFLICT(%s, facet_id) DO UPDATE SET
			  namespace_id = excluded.namespace_id,
			  dimension = excluded.dimension,
			  embedding = excluded.embedding,
			  updated_at = excluded.updated_at`, spec.vectorTable, spec.idColumn, spec.idColumn)
	}
	return fmt.Sprintf(`
		INSERT INTO %s (%s, namespace_id, dimension, embedding, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(%s) DO UPDATE SET
		  namespace_id = excluded.namespace_id,
		  dimension = excluded.dimension,
		  embedding = excluded.embedding,
		  updated_at = excluded.updated_at`, spec.vectorTable, spec.idColumn, spec.idColumn)
}

func hnswSpecForKind(k VectorKind) (hnswTableSpec, error) {
	if k == "" {
		k = VectorKindMemory
	}
	spec, ok := hnswSpecs[k]
	if !ok {
		return hnswTableSpec{}, fmt.Errorf("hnsw: unknown vector kind %q", k)
	}
	return spec, nil
}

// Upsert inserts or updates a single vector associated with a memory or entity.
func (s *HNSWStore) Upsert(ctx context.Context, kind VectorKind, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error {
	if !SupportedVectorDimensions[dimension] {
		return fmt.Errorf("hnsw: unsupported dimension %d", dimension)
	}
	spec, err := hnswSpecForKind(kind)
	if err != nil {
		return err
	}

	encoded := hnsw.EncodeVector(embedding)
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

	_, err = s.writeDB.ExecContext(ctx, hnswUpsertSQL(spec),
		id.String(), namespaceID.String(), dimension, encoded, now, now,
	)
	if err != nil {
		return fmt.Errorf("hnsw: upsert %s: %w", spec.vectorTable, err)
	}

	graph, err := s.cache.GetOrCreate(ctx, spec.cacheKind, namespaceID, dimension)
	if err != nil {
		return fmt.Errorf("hnsw: get or create index: %w", err)
	}

	// HNSW Add handles update semantics internally (removes existing node first).
	if err := graph.Add(hnsw.Node{ID: id, Vector: embedding}); err != nil {
		return fmt.Errorf("hnsw: add node: %w", err)
	}

	s.cache.MarkDirty(spec.cacheKind, namespaceID, dimension)
	return nil
}

// UpsertBatch inserts or updates multiple vectors in a single operation.
// Items group by (kind, namespace, dimension) so each HNSW partition is loaded once.
func (s *HNSWStore) UpsertBatch(ctx context.Context, items []VectorUpsertItem) error {
	if len(items) == 0 {
		return nil
	}

	// Validate dimensions and resolve specs first.
	for _, item := range items {
		if !SupportedVectorDimensions[item.Dimension] {
			return fmt.Errorf("hnsw: unsupported dimension %d", item.Dimension)
		}
		if _, err := hnswSpecForKind(item.EffectiveKind()); err != nil {
			return err
		}
	}

	// Group items by (kind, namespaceID, dimension).
	type groupKey struct {
		Kind        VectorKind
		NamespaceID uuid.UUID
		Dimension   int
	}
	groups := make(map[groupKey][]VectorUpsertItem)
	for _, item := range items {
		key := groupKey{Kind: item.EffectiveKind(), NamespaceID: item.NamespaceID, Dimension: item.Dimension}
		groups[key] = append(groups[key], item)
	}

	for gk, group := range groups {
		spec, _ := hnswSpecForKind(gk.Kind) // already validated above

		// Insert all SQLite rows in a transaction.
		tx, err := s.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("hnsw: begin transaction: %w", err)
		}

		now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		stmt, err := tx.PrepareContext(ctx, hnswUpsertSQL(spec))
		if err != nil {
			tx.Rollback() //nolint:errcheck
			return fmt.Errorf("hnsw: prepare batch insert: %w", err)
		}

		// Track which items were actually committed so the in-memory HNSW
		// graph only adds nodes whose SQLite row exists. Items skipped due
		// to a FK violation (parent row was concurrently deleted) are
		// dropped from the in-memory index too.
		committed := make([]VectorUpsertItem, 0, len(group))
		var skipped int
		for _, item := range group {
			encoded := hnsw.EncodeVector(item.Embedding)
			_, err := stmt.ExecContext(ctx, item.ID.String(), item.NamespaceID.String(), item.Dimension, encoded, now, now)
			if err != nil {
				if isForeignKeyViolation(err) {
					// Parent row (memories or entities) was deleted between
					// the producer (enrichment / dream) creating the parent
					// and this vector insert. With ON DELETE CASCADE on the
					// vector tables, the resulting state (no parent row, no
					// vector row) is the intended steady state. Skip this
					// item and move on; the rest of the batch is still
					// healthy and committable.
					slog.Warn("hnsw: skipping vector with missing parent row",
						"table", spec.vectorTable,
						"id", item.ID,
						"namespace_id", item.NamespaceID,
						"reason", "foreign_key_violation_likely_lifecycle_race",
					)
					skipped++
					continue
				}
				stmt.Close()  //nolint:errcheck
				tx.Rollback() //nolint:errcheck
				return fmt.Errorf("hnsw: batch insert item %s: %w", item.ID, err)
			}
			committed = append(committed, item)
		}
		stmt.Close() //nolint:errcheck

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("hnsw: commit batch insert: %w", err)
		}
		if skipped > 0 {
			slog.Warn("hnsw: batch upsert skipped items",
				"table", spec.vectorTable,
				"committed", len(committed),
				"skipped", skipped,
			)
		}
		if len(committed) == 0 {
			continue
		}

		// Load the HNSW index and add only the committed nodes.
		graph, err := s.cache.GetOrCreate(ctx, spec.cacheKind, gk.NamespaceID, gk.Dimension)
		if err != nil {
			return fmt.Errorf("hnsw: get or create index for batch: %w", err)
		}

		for _, item := range committed {
			if err := graph.Add(hnsw.Node{ID: item.ID, Vector: item.Embedding}); err != nil {
				return fmt.Errorf("hnsw: batch add node %s: %w", item.ID, err)
			}
		}

		s.cache.MarkDirty(spec.cacheKind, gk.NamespaceID, gk.Dimension)
	}

	return nil
}

// UpsertFacets atomically replaces a memory's facet set at the given dimension.
// facets[0] is facet 0 (the pooled whole-memory vector) and is the only facet
// indexed in the in-memory graph; facets[1:] are topic facets stored in SQLite
// and brute-forced at search time. An empty slice removes all facets and the
// graph node.
func (s *HNSWStore) UpsertFacets(ctx context.Context, memoryID uuid.UUID, namespaceID uuid.UUID, dimension int, facets [][]float32) error {
	if !SupportedVectorDimensions[dimension] {
		return fmt.Errorf("hnsw: unsupported dimension %d", dimension)
	}
	spec := hnswSpecs[VectorKindMemory]
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("hnsw: upsert-facets begin: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s = ? AND dimension = ?", spec.vectorTable, spec.idColumn),
		memoryID.String(), dimension); err != nil {
		tx.Rollback() //nolint:errcheck
		return fmt.Errorf("hnsw: upsert-facets clear: %w", err)
	}
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s, facet_id, namespace_id, dimension, embedding, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		spec.vectorTable, spec.idColumn)
	for i, vec := range facets {
		if _, err := tx.ExecContext(ctx, insertSQL,
			memoryID.String(), i, namespaceID.String(), dimension, hnsw.EncodeVector(vec), now, now); err != nil {
			tx.Rollback() //nolint:errcheck
			if isForeignKeyViolation(err) {
				// Parent memory deleted concurrently (lifecycle race); the
				// cascade leaves the intended no-parent/no-vector steady state.
				return nil
			}
			return fmt.Errorf("hnsw: upsert-facets insert facet %d: %w", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("hnsw: upsert-facets commit: %w", err)
	}
	// The topic-facet set for this namespace/dimension just changed; drop the
	// cached presence answer so the next recall re-probes instead of waiting for
	// the TTL (this is the no-facets -> has-facets transition that matters).
	s.gate.invalidate(namespaceID, dimension)

	// Reflect facet 0 in the in-memory graph (topic facets are SQLite-only).
	graph, err := s.cache.GetOrCreate(ctx, spec.cacheKind, namespaceID, dimension)
	if err != nil {
		// Persisted in SQLite; the graph will be correct on next rebuild.
		return nil //nolint:nilerr
	}
	graph.Delete(memoryID)
	if len(facets) > 0 {
		if err := graph.Add(hnsw.Node{ID: memoryID, Vector: facets[0]}); err != nil {
			return fmt.Errorf("hnsw: upsert-facets add facet-0 node: %w", err)
		}
	}
	s.cache.MarkDirty(spec.cacheKind, namespaceID, dimension)
	return nil
}

// Search finds the nearest neighbor vectors within a namespace, returning up to
// topK results. For the faceted memory kind it merges the graph (facet 0) hits
// with a brute-force scan of the topic facets (facet_id > 0) and collapses to
// the best facet per memory, so a query matching a memory's sub-topic surfaces
// it at that facet's strength.
func (s *HNSWStore) Search(ctx context.Context, kind VectorKind, embedding []float32, namespaceID uuid.UUID, dimension int, topK int) ([]VectorSearchResult, error) {
	if !SupportedVectorDimensions[dimension] {
		return nil, fmt.Errorf("hnsw: unsupported dimension %d", dimension)
	}
	spec, err := hnswSpecForKind(kind)
	if err != nil {
		return nil, err
	}

	graph, err := s.cache.GetOrCreate(ctx, spec.cacheKind, namespaceID, dimension)
	if err != nil {
		return nil, fmt.Errorf("hnsw: get or create index for search: %w", err)
	}

	// Best score per id across facet 0 (graph) and topic facets (brute force).
	best := make(map[uuid.UUID]float64)
	graphTopK := topK
	if spec.faceted {
		graphTopK = topK * facetSearchOverFetch
	}
	if graph.Len() > 0 {
		results, err := graph.Search(embedding, graphTopK)
		if err != nil {
			return nil, fmt.Errorf("hnsw: search: %w", err)
		}
		for _, r := range results {
			if cur, ok := best[r.ID]; !ok || r.Score > cur {
				best[r.ID] = r.Score
			}
		}
	}

	if spec.faceted {
		active := s.gate.active(ctx, namespaceID, dimension, func(pctx context.Context) (bool, error) {
			return s.hasTopicFacets(pctx, namespaceID, dimension)
		})
		if active {
			if err := s.scanFacetCandidates(ctx, spec, embedding, namespaceID, dimension, best); err != nil {
				return nil, err
			}
		}
	}

	return collapseFacets(best, namespaceID, topK), nil
}

// hasTopicFacets reports whether the namespace/dimension holds any topic-facet
// (facet_id > 0) rows. An O(log n) index probe served by idx_memory_vectors_ns_dim;
// used by the facet gate to skip the brute-force scan when there is nothing to scan.
func (s *HNSWStore) hasTopicFacets(ctx context.Context, namespaceID uuid.UUID, dimension int) (bool, error) {
	spec := hnswSpecs[VectorKindMemory]
	var one int
	err := s.readDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT 1 FROM %s WHERE namespace_id = ? AND dimension = ? AND facet_id > 0 LIMIT 1", spec.vectorTable),
		namespaceID.String(), dimension).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("hnsw: probe topic facets: %w", err)
	}
	return true, nil
}

// foldBestFacetCosine scans (id, embedding-blob) rows, decodes each vector, scores
// it against query, and folds the maximum cosine per id into out. queryNorm is
// passed in (the query is constant across the scan, so its norm is computed once)
// and CosineSimilarityWithNorms makes each row pay only its own Norm. Shared by
// the two SQLite facet readers — scanFacetCandidates (topic facets, for Search)
// and BestFacetCosines (all facets, by id) — which differ only in their SQL.
func foldBestFacetCosine(rows *sql.Rows, query []float32, queryNorm float32, out map[uuid.UUID]float64) error {
	for rows.Next() {
		var idStr string
		var blob []byte
		if err := rows.Scan(&idStr, &blob); err != nil {
			return fmt.Errorf("hnsw: scan facet row: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return fmt.Errorf("hnsw: parse facet memory_id %q: %w", idStr, err)
		}
		vec, err := hnsw.DecodeVector(blob)
		if err != nil {
			return fmt.Errorf("hnsw: decode facet vector for %s: %w", id, err)
		}
		score := hnsw.CosineSimilarityWithNorms(query, vec, queryNorm, hnsw.Norm(vec))
		if cur, ok := out[id]; !ok || score > cur {
			out[id] = score
		}
	}
	return rows.Err()
}

// scanFacetCandidates brute-forces the topic facets (facet_id > 0) for a
// namespace/dimension, folding the best cosine per memory into best. This is the
// SQLite brute-force backend's multi-vector path; topic facets are not in the
// HNSW graph.
func (s *HNSWStore) scanFacetCandidates(ctx context.Context, spec hnswTableSpec, embedding []float32, namespaceID uuid.UUID, dimension int, best map[uuid.UUID]float64) error {
	s.scanCount.Add(1)
	rows, err := s.readDB.QueryContext(ctx,
		fmt.Sprintf("SELECT %s, embedding FROM %s WHERE namespace_id = ? AND dimension = ? AND facet_id > 0", spec.idColumn, spec.vectorTable),
		namespaceID.String(), dimension)
	if err != nil {
		return fmt.Errorf("hnsw: scan facets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if err := foldBestFacetCosine(rows, embedding, hnsw.Norm(embedding), best); err != nil {
		return fmt.Errorf("hnsw: iterate facets: %w", err)
	}
	return nil
}

// BestFacetCosines folds each id's facets to its single best cosine against the
// query. Unlike GetByIDs (which returns facet 0 from the graph), this reads the
// embedding blobs straight from the vector table with no facet filter, so the
// pooled facet 0 and every topic facet are scored and the maximum kept — the
// by-id analogue of scanFacetCandidates' max-over-facets fold. The query norm is
// constant across the scan, so it is computed once.
func (s *HNSWStore) BestFacetCosines(ctx context.Context, kind VectorKind, ids []uuid.UUID, query []float32, dimension int) (map[uuid.UUID]float64, error) {
	out := make(map[uuid.UUID]float64, len(ids))
	if len(ids) == 0 || len(query) == 0 {
		return out, nil
	}
	if !SupportedVectorDimensions[dimension] {
		return nil, fmt.Errorf("hnsw: unsupported dimension %d", dimension)
	}
	spec, err := hnswSpecForKind(kind)
	if err != nil {
		return nil, err
	}

	placeholders := make([]byte, 0, 2*len(ids))
	args := make([]any, 0, len(ids)+1)
	args = append(args, dimension)
	for i, id := range ids {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id.String())
	}
	// No facet filter: every facet row (facet 0 + topic facets) is scored so the
	// max reflects the best-matching facet, exactly as Search ranks.
	q := fmt.Sprintf("SELECT %s, embedding FROM %s WHERE dimension = ? AND %s IN (",
		spec.idColumn, spec.vectorTable, spec.idColumn) + string(placeholders) + ")"
	rows, err := s.readDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("hnsw: best-facet-cosines lookup: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if err := foldBestFacetCosine(rows, query, hnsw.Norm(query), out); err != nil {
		return nil, fmt.Errorf("hnsw: iterate best-facet-cosines: %w", err)
	}
	return out, nil
}

// GetByIDs resolves namespace_id from the kind's vector table first, then
// copies vectors out of each loaded graph; the HNSW index is partitioned by
// (kind, namespace_id, dimension) but callers pass a flat ID list.
func (s *HNSWStore) GetByIDs(ctx context.Context, kind VectorKind, ids []uuid.UUID, dimension int) (map[uuid.UUID][]float32, error) {
	if len(ids) == 0 {
		return map[uuid.UUID][]float32{}, nil
	}
	if !SupportedVectorDimensions[dimension] {
		return nil, fmt.Errorf("hnsw: unsupported dimension %d", dimension)
	}
	spec, err := hnswSpecForKind(kind)
	if err != nil {
		return nil, err
	}

	// Build a placeholder list and arg slice for the IN clause. Bounded by
	// the caller (one dream cycle's full namespace ≤ ListByNamespace's 500
	// limit), so a single query is fine.
	placeholders := make([]byte, 0, 2*len(ids))
	args := make([]any, 0, len(ids)+1)
	args = append(args, dimension)
	for i, id := range ids {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id.String())
	}

	// Faceted (memory) rows: callers want the pooled whole-memory vector (facet
	// 0), and the graph only holds facet 0 anyway.
	facetFilter := ""
	if spec.faceted {
		facetFilter = " AND facet_id = 0"
	}
	query := fmt.Sprintf("SELECT %s, namespace_id FROM %s WHERE dimension = ?%s AND %s IN (",
		spec.idColumn, spec.vectorTable, facetFilter, spec.idColumn) + string(placeholders) + ")"
	rows, err := s.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("hnsw: get-by-ids lookup: %w", err)
	}

	type idRef struct {
		id uuid.UUID
		ns uuid.UUID
	}
	byNamespace := make(map[uuid.UUID][]idRef)
	for rows.Next() {
		var idStr, nsStr string
		if err := rows.Scan(&idStr, &nsStr); err != nil {
			rows.Close() //nolint:errcheck
			return nil, fmt.Errorf("hnsw: get-by-ids scan: %w", err)
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			rows.Close() //nolint:errcheck
			return nil, fmt.Errorf("hnsw: parse %s %q: %w", spec.idColumn, idStr, err)
		}
		ns, err := uuid.Parse(nsStr)
		if err != nil {
			rows.Close() //nolint:errcheck
			return nil, fmt.Errorf("hnsw: parse namespace_id %q: %w", nsStr, err)
		}
		byNamespace[ns] = append(byNamespace[ns], idRef{id: id, ns: ns})
	}
	rows.Close() //nolint:errcheck
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("hnsw: get-by-ids rows: %w", err)
	}

	out := make(map[uuid.UUID][]float32, len(ids))
	for ns, refs := range byNamespace {
		graph, err := s.cache.GetOrCreate(ctx, spec.cacheKind, ns, dimension)
		if err != nil {
			return nil, fmt.Errorf("hnsw: get-by-ids load index for kind=%s ns=%s dim=%d: %w", kind, ns, dimension, err)
		}
		want := make([]uuid.UUID, len(refs))
		for i, r := range refs {
			want[i] = r.id
		}
		got := graph.GetVectors(want)
		maps.Copy(out, got)
	}
	return out, nil
}

// Delete removes a vector by its associated parent ID.
func (s *HNSWStore) Delete(ctx context.Context, kind VectorKind, id uuid.UUID) error {
	spec, err := hnswSpecForKind(kind)
	if err != nil {
		return err
	}

	// Look up the row to get namespace_id and dimension.
	var nsIDStr string
	var dimension int
	err = s.readDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT namespace_id, dimension FROM %s WHERE %s = ?", spec.vectorTable, spec.idColumn),
		id.String(),
	).Scan(&nsIDStr, &dimension)
	if err == sql.ErrNoRows {
		return nil // Already deleted.
	}
	if err != nil {
		return fmt.Errorf("hnsw: lookup %s for delete: %w", spec.vectorTable, err)
	}

	nsID, err := uuid.Parse(nsIDStr)
	if err != nil {
		return fmt.Errorf("hnsw: parse namespace_id %q: %w", nsIDStr, err)
	}

	// Delete from SQLite.
	_, err = s.writeDB.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s = ?", spec.vectorTable, spec.idColumn),
		id.String())
	if err != nil {
		return fmt.Errorf("hnsw: delete from %s: %w", spec.vectorTable, err)
	}

	// Remove from the HNSW index if it's loaded in cache.
	// We use GetOrCreate to check: if the graph is loaded it's a fast cache hit.
	// If it's not loaded, we load it (which will reflect the deletion from SQLite).
	graph, err := s.cache.GetOrCreate(ctx, spec.cacheKind, nsID, dimension)
	if err != nil {
		// The deletion is persisted in SQLite; the graph will be correct on next load.
		return nil
	}

	graph.Delete(id)
	s.cache.MarkDirty(spec.cacheKind, nsID, dimension)

	return nil
}

// DeleteByNamespaceTx removes all HNSW snapshots for a given namespace
// (across both memory and entity kinds) inside the caller's transaction. The
// cache is evicted first so the background flush cannot re-insert rows that
// are about to be deleted; the snapshot row deletes run on the caller's tx
// so they commit atomically with the namespace row delete.
func (s *HNSWStore) DeleteByNamespaceTx(ctx context.Context, tx *sql.Tx, namespaceID uuid.UUID) error {
	s.cache.RemoveByNamespace(namespaceID)

	for _, spec := range hnswSpecs {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE namespace_id = ?", spec.snapshotTable),
			namespaceID.String(),
		); err != nil {
			return fmt.Errorf("hnsw: delete snapshots from %s by namespace: %w", spec.snapshotTable, err)
		}
	}
	return nil
}

// TruncateAllVectors evicts every cached graph and clears every persisted
// vector and snapshot row. HNSW stores all dims in a single vector table
// per kind (with a dimension column), so one DELETE per table covers
// every dim.
func (s *HNSWStore) TruncateAllVectors(ctx context.Context) error {
	// Evict cache first so the background flush cannot re-insert stale
	// graphs after the wipe.
	s.cache.RemoveAll()

	for _, spec := range hnswSpecs {
		if _, err := s.writeDB.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s", spec.vectorTable)); err != nil {
			return fmt.Errorf("hnsw: truncate %s: %w", spec.vectorTable, err)
		}
		if _, err := s.writeDB.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s", spec.snapshotTable)); err != nil {
			return fmt.Errorf("hnsw: truncate %s: %w", spec.snapshotTable, err)
		}
	}
	// Every namespace just lost its facets; clear the presence cache so none
	// keeps a stale "has facets" answer.
	s.gate.invalidateAll()
	return nil
}

// Ping checks vector store connectivity by pinging the underlying SQLite database.
func (s *HNSWStore) Ping(ctx context.Context) error {
	return s.readDB.PingContext(ctx)
}

// Close stops the background snapshot goroutine and flushes all dirty snapshots.
func (s *HNSWStore) Close() error {
	return s.cache.Close()
}
