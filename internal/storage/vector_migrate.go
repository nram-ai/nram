package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// OrderedVectorDimensions lists the supported embedding dimensions in ascending
// order for deterministic migration iteration. It mirrors the keys of
// SupportedVectorDimensions; the slice form gives a stable traversal order.
var OrderedVectorDimensions = []int{384, 512, 768, 1024, 1536, 3072}

// MemoryVectorCount returns the number of memory vectors held in the SQL primary
// store, summed across the per-dimension Postgres tables or read from the single
// SQLite table. Best-effort: scan errors count as zero. Used by the startup
// activation guard and the database-info admin view, which previously each
// hand-rolled this query.
func MemoryVectorCount(ctx context.Context, db DB) int {
	if db.Backend() == BackendSQLite {
		var n int
		_ = db.QueryRow(ctx, "SELECT COUNT(*) FROM memory_vectors").Scan(&n)
		return n
	}
	total := 0
	for _, dim := range OrderedVectorDimensions {
		var n int
		if db.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM memory_vectors_%d", dim)).Scan(&n) == nil {
			total += n
		}
	}
	return total
}

// VectorDimVerify reports per-(kind, dimension) source and destination counts so
// a partial copy is detectable after a migration run rather than silently
// passing as success.
type VectorDimVerify struct {
	Kind        string `json:"kind"`
	Dimension   int    `json:"dimension"`
	SourceCount int    `json:"source_count"`
	DestCount   int    `json:"dest_count"`
}

// VectorMigrateStats summarizes a vector migration run. Counts are source-row
// counts (what was read and, on a real run, copied). Verify carries the
// per-dimension source-vs-destination comparison.
type VectorMigrateStats struct {
	MemoryCount int               `json:"memory_count"`
	EntityCount int               `json:"entity_count"`
	DryRun      bool              `json:"dry_run"`
	Verify      []VectorDimVerify `json:"verify"`
}

// Mismatch reports whether any verified dimension ended with fewer destination
// rows than source rows after a real (non-dry-run) copy, indicating an
// incomplete migration. UpsertBatch errors abort the run before this matters;
// Mismatch is a secondary, conservative completeness check.
func (s VectorMigrateStats) Mismatch() bool {
	if s.DryRun {
		return false
	}
	for _, v := range s.Verify {
		if v.DestCount < v.SourceCount {
			return true
		}
	}
	return false
}

// batchUpserter is the single write primitive the migrator needs from a
// destination. Both VectorStore (pgvector, HNSW) and *QdrantStore satisfy it.
type batchUpserter interface {
	UpsertBatch(ctx context.Context, items []VectorUpsertItem) error
}

// qdrantTarget is the subset of *QdrantStore the to-Qdrant migration uses.
// Narrowed to an interface so the migrator can be unit-tested with a fake
// destination without a live Qdrant server.
type qdrantTarget interface {
	EnsureCollections(ctx context.Context) error
	UpsertBatch(ctx context.Context, items []VectorUpsertItem) error
	CountVectors(ctx context.Context, kind VectorKind, dimension int) (int, error)
}

// qdrantSource is the subset of *QdrantStore the from-Qdrant migration uses.
type qdrantSource interface {
	IterateVectors(ctx context.Context, kind VectorKind, dimension int, batchSize int, fn func(id, namespaceID uuid.UUID, vec []float32) error) error
}

// VectorMigrateProgress is emitted after each flushed batch so a caller can
// stream live progress. Memory/Entity copied are cumulative for the whole run.
type VectorMigrateProgress struct {
	Direction    string `json:"direction"`
	Kind         string `json:"kind"`
	Dimension    int    `json:"dimension"`
	MemoryCopied int    `json:"memory_copied"`
	EntityCopied int    `json:"entity_copied"`
}

// upsertBatcher buffers VectorUpsertItems and flushes them to a destination in
// fixed-size batches. On a dry run it discards everything so no write ever
// reaches the destination. onFlush, when set, receives the number of items in
// each committed batch (never called on a dry run).
type upsertBatcher struct {
	dst       batchUpserter
	batchSize int
	dryRun    bool
	buf       []VectorUpsertItem
	onFlush   func(n int)
}

func (b *upsertBatcher) add(ctx context.Context, item VectorUpsertItem) error {
	if b.dryRun {
		return nil
	}
	b.buf = append(b.buf, item)
	if len(b.buf) >= b.batchSize {
		return b.flush(ctx)
	}
	return nil
}

func (b *upsertBatcher) flush(ctx context.Context) error {
	n := len(b.buf)
	if b.dryRun || n == 0 {
		b.buf = b.buf[:0]
		return nil
	}
	if err := b.dst.UpsertBatch(ctx, b.buf); err != nil {
		return err
	}
	b.buf = b.buf[:0]
	if b.onFlush != nil {
		b.onFlush(n)
	}
	return nil
}

// MigrateVectorsToQdrant copies every memory and entity vector from the SQL
// primary store (pgvector or SQLite/HNSW) into Qdrant, across all supported
// dimensions. It reads the SQL source read-only and writes only through
// QdrantStore.UpsertBatch; it never deletes and is safely re-runnable. On a dry
// run it counts source rows without writing.
func MigrateVectorsToQdrant(ctx context.Context, db DB, dst qdrantTarget, batchSize int, dryRun bool, onProgress func(VectorMigrateProgress)) (VectorMigrateStats, error) {
	stats := VectorMigrateStats{DryRun: dryRun}
	if batchSize <= 0 {
		batchSize = 1000
	}
	if dst == nil {
		return stats, fmt.Errorf("storage: migrate-to-qdrant: nil destination store")
	}
	if !dryRun {
		if err := dst.EnsureCollections(ctx); err != nil {
			return stats, fmt.Errorf("storage: migrate-to-qdrant: ensure collections: %w", err)
		}
	}

	memCopied, entCopied := 0, 0
	for _, kind := range []VectorKind{VectorKindMemory, VectorKindEntity} {
		for _, dim := range OrderedVectorDimensions {
			batcher := &upsertBatcher{dst: dst, batchSize: batchSize, dryRun: dryRun, onFlush: func(n int) {
				if kind == VectorKindEntity {
					entCopied += n
				} else {
					memCopied += n
				}
				if onProgress != nil {
					onProgress(VectorMigrateProgress{
						Direction: "to_qdrant", Kind: string(kind), Dimension: dim,
						MemoryCopied: memCopied, EntityCopied: entCopied,
					})
				}
			}}
			srcCount, err := streamSQLVectors(ctx, db, kind, dim, func(item VectorUpsertItem) error {
				return batcher.add(ctx, item)
			})
			if err != nil {
				return stats, err
			}
			if err := batcher.flush(ctx); err != nil {
				return stats, fmt.Errorf("storage: migrate-to-qdrant: upsert kind=%s dim=%d: %w", kind, dim, err)
			}
			if srcCount == 0 {
				continue
			}
			if kind == VectorKindEntity {
				stats.EntityCount += srcCount
			} else {
				stats.MemoryCount += srcCount
			}
			// A dry run writes nothing, so the destination is unchanged and the
			// per-dimension verification (a Qdrant round trip each) is wasted.
			if dryRun {
				continue
			}
			dstCount, err := dst.CountVectors(ctx, kind, dim)
			if err != nil {
				return stats, fmt.Errorf("storage: migrate-to-qdrant: verify count kind=%s dim=%d: %w", kind, dim, err)
			}
			stats.Verify = append(stats.Verify, VectorDimVerify{
				Kind: string(kind), Dimension: dim, SourceCount: srcCount, DestCount: dstCount,
			})
		}
	}
	return stats, nil
}

// MigrateVectorsFromQdrant copies every memory and entity vector from Qdrant
// back into the SQL primary store. The destination dst must be the SQL-backed
// VectorStore for the running backend (PgVectorStore or HNSWStore); writing
// through its UpsertBatch keeps the pgvector / HNSW index consistent. It reads
// Qdrant read-only, never deletes, and is safely re-runnable.
//
// Note: Qdrant normalizes vectors on insert for Cosine collections, so the
// values copied back are unit-normalized rather than byte-identical to what was
// originally embedded. Every nram backend ranks by cosine similarity, which is
// scale-invariant, so search results are unaffected; only the stored magnitude
// differs.
func MigrateVectorsFromQdrant(ctx context.Context, db DB, src qdrantSource, dst VectorStore, batchSize int, dryRun bool, onProgress func(VectorMigrateProgress)) (VectorMigrateStats, error) {
	stats := VectorMigrateStats{DryRun: dryRun}
	if batchSize <= 0 {
		batchSize = 1000
	}
	if src == nil {
		return stats, fmt.Errorf("storage: migrate-from-qdrant: nil source store")
	}
	if dst == nil {
		return stats, fmt.Errorf("storage: migrate-from-qdrant: nil destination store")
	}

	memCopied, entCopied := 0, 0
	for _, kind := range []VectorKind{VectorKindMemory, VectorKindEntity} {
		for _, dim := range OrderedVectorDimensions {
			batcher := &upsertBatcher{dst: dst, batchSize: batchSize, dryRun: dryRun, onFlush: func(n int) {
				if kind == VectorKindEntity {
					entCopied += n
				} else {
					memCopied += n
				}
				if onProgress != nil {
					onProgress(VectorMigrateProgress{
						Direction: "from_qdrant", Kind: string(kind), Dimension: dim,
						MemoryCopied: memCopied, EntityCopied: entCopied,
					})
				}
			}}
			srcCount := 0
			err := src.IterateVectors(ctx, kind, dim, batchSize, func(id, ns uuid.UUID, vec []float32) error {
				if len(vec) != dim {
					return fmt.Errorf("storage: migrate-from-qdrant: point %s has %d dims, expected %d", id, len(vec), dim)
				}
				srcCount++
				return batcher.add(ctx, VectorUpsertItem{
					Kind: kind, ID: id, NamespaceID: ns, Embedding: vec, Dimension: dim,
				})
			})
			if err != nil {
				return stats, fmt.Errorf("storage: migrate-from-qdrant: read kind=%s dim=%d: %w", kind, dim, err)
			}
			if err := batcher.flush(ctx); err != nil {
				return stats, fmt.Errorf("storage: migrate-from-qdrant: upsert kind=%s dim=%d: %w", kind, dim, err)
			}
			if srcCount == 0 {
				continue
			}
			if kind == VectorKindEntity {
				stats.EntityCount += srcCount
			} else {
				stats.MemoryCount += srcCount
			}
			// A dry run writes nothing, so the destination is unchanged and the
			// per-dimension verification is wasted.
			if dryRun {
				continue
			}
			dstCount, err := countSQLVectors(ctx, db, kind, dim)
			if err != nil {
				return stats, fmt.Errorf("storage: migrate-from-qdrant: verify count kind=%s dim=%d: %w", kind, dim, err)
			}
			stats.Verify = append(stats.Verify, VectorDimVerify{
				Kind: string(kind), Dimension: dim, SourceCount: srcCount, DestCount: dstCount,
			})
		}
	}
	return stats, nil
}

// streamSQLVectors reads every source vector for (kind, dim) from the SQL store
// and invokes fn per row. It is read-only and validates that each decoded
// vector has the expected dimension. Returns the number of rows streamed.
func streamSQLVectors(ctx context.Context, db DB, kind VectorKind, dim int, fn func(VectorUpsertItem) error) (int, error) {
	if !SupportedVectorDimensions[dim] {
		return 0, fmt.Errorf("storage: unsupported vector dimension %d", dim)
	}
	k := kind
	if k == "" {
		k = VectorKindMemory
	}
	switch db.Backend() {
	case BackendPostgres:
		return streamPgVectors(ctx, db, k, dim, fn)
	case BackendSQLite:
		return streamSQLiteVectors(ctx, db, k, dim, fn)
	default:
		return 0, fmt.Errorf("storage: unknown backend %q", db.Backend())
	}
}

func streamPgVectors(ctx context.Context, db DB, kind VectorKind, dim int, fn func(VectorUpsertItem) error) (int, error) {
	spec, err := resolveTableSpec(kind, dim)
	if err != nil {
		return 0, err
	}
	where := ""
	if spec.softDeletes {
		where = " WHERE p.deleted_at IS NULL"
	}
	query := fmt.Sprintf(
		`SELECT v.%s, p.namespace_id, v.embedding::text FROM %s v JOIN %s p ON v.%s = p.id%s`,
		spec.idColumn, spec.table, spec.parent, spec.idColumn, where,
	)
	rows, err := db.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("storage: read %s: %w", spec.table, err)
	}
	defer func() { _ = rows.Close() }()

	count := 0
	for rows.Next() {
		var idStr, nsStr, embText string
		if err := rows.Scan(&idStr, &nsStr, &embText); err != nil {
			return count, fmt.Errorf("storage: scan %s: %w", spec.table, err)
		}
		vec, perr := parsePgVectorText(embText)
		item, err := makeVectorItem(kind, dim, idStr, nsStr, vec, perr)
		if err != nil {
			return count, err
		}
		count++
		if err := fn(item); err != nil {
			return count, err
		}
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("storage: iterate %s: %w", spec.table, err)
	}
	return count, nil
}

func streamSQLiteVectors(ctx context.Context, db DB, kind VectorKind, dim int, fn func(VectorUpsertItem) error) (int, error) {
	table, idCol, parent, soft, err := sqliteVectorSpec(kind)
	if err != nil {
		return 0, err
	}
	var query string
	if soft {
		query = fmt.Sprintf(
			`SELECT v.%s, v.namespace_id, v.embedding FROM %s v JOIN %s p ON v.%s = p.id WHERE p.deleted_at IS NULL AND v.dimension = ?`,
			idCol, table, parent, idCol,
		)
	} else {
		query = fmt.Sprintf(`SELECT %s, namespace_id, embedding FROM %s WHERE dimension = ?`, idCol, table)
	}
	rows, err := db.Query(ctx, query, dim)
	if err != nil {
		return 0, fmt.Errorf("storage: read %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	count := 0
	for rows.Next() {
		var idStr, nsStr string
		var blob []byte
		if err := rows.Scan(&idStr, &nsStr, &blob); err != nil {
			return count, fmt.Errorf("storage: scan %s: %w", table, err)
		}
		vec, derr := hnsw.DecodeVector(blob)
		item, err := makeVectorItem(kind, dim, idStr, nsStr, vec, derr)
		if err != nil {
			return count, err
		}
		count++
		if err := fn(item); err != nil {
			return count, err
		}
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("storage: iterate %s: %w", table, err)
	}
	return count, nil
}

// countSQLVectors counts rows in the SQL vector table for (kind, dim). Used to
// verify the destination after a reverse migration.
func countSQLVectors(ctx context.Context, db DB, kind VectorKind, dim int) (int, error) {
	switch db.Backend() {
	case BackendPostgres:
		spec, err := resolveTableSpec(kind, dim)
		if err != nil {
			return 0, err
		}
		var n int
		if err := db.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", spec.table)).Scan(&n); err != nil {
			return 0, fmt.Errorf("storage: count %s: %w", spec.table, err)
		}
		return n, nil
	case BackendSQLite:
		table, _, _, _, err := sqliteVectorSpec(kind)
		if err != nil {
			return 0, err
		}
		var n int
		if err := db.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE dimension = ?", table), dim).Scan(&n); err != nil {
			return 0, fmt.Errorf("storage: count %s: %w", table, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("storage: unknown backend %q", db.Backend())
	}
}

// sqliteVectorSpec returns the SQLite table, id column, parent table, and
// whether the parent is soft-deletable, for the given vector kind.
func sqliteVectorSpec(kind VectorKind) (table, idCol, parent string, soft bool, err error) {
	switch kind {
	case "", VectorKindMemory:
		return "memory_vectors", "memory_id", "memories", true, nil
	case VectorKindEntity:
		return "entity_vectors", "entity_id", "entities", false, nil
	default:
		return "", "", "", false, fmt.Errorf("storage: unknown vector kind %q", kind)
	}
}

// makeVectorItem validates a decoded vector and parses its identifiers into a
// VectorUpsertItem. A parse/decode error or a dimension mismatch is fatal so a
// corrupt source row cannot pass silently.
func makeVectorItem(kind VectorKind, dim int, idStr, nsStr string, vec []float32, decodeErr error) (VectorUpsertItem, error) {
	if decodeErr != nil {
		return VectorUpsertItem{}, fmt.Errorf("storage: decode vector for %s: %w", idStr, decodeErr)
	}
	if len(vec) != dim {
		return VectorUpsertItem{}, fmt.Errorf("storage: vector for %s has %d dims, expected %d", idStr, len(vec), dim)
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return VectorUpsertItem{}, fmt.Errorf("storage: invalid id %q: %w", idStr, err)
	}
	ns, err := uuid.Parse(nsStr)
	if err != nil {
		return VectorUpsertItem{}, fmt.Errorf("storage: invalid namespace_id %q for %s: %w", nsStr, idStr, err)
	}
	return VectorUpsertItem{Kind: kind, ID: id, NamespaceID: ns, Embedding: vec, Dimension: dim}, nil
}

// parsePgVectorText parses pgvector's text output "[0.1,0.2,0.3]" into a
// []float32. Empty content "[]" yields an empty slice.
func parsePgVectorText(s string) ([]float32, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf("storage: invalid embedding text format: expected [...]")
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return []float32{}, nil
	}
	parts := strings.Split(inner, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("storage: invalid float at index %d: %w", i, err)
		}
		out[i] = float32(v)
	}
	return out, nil
}
