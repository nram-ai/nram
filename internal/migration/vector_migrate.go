package migration

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
)

// vectorDimensions lists all supported embedding dimensions in ascending order.
var vectorDimensions = []int{384, 512, 768, 1024, 1536, 3072}

// vectorMigrateArgs holds the parsed arguments for the migrate-vectors command.
type vectorMigrateArgs struct {
	QdrantAddr string
	BatchSize  int
	DryRun     bool
}

// parseVectorMigrateArgs parses the CLI arguments for the migrate-vectors command.
func parseVectorMigrateArgs(args []string) (vectorMigrateArgs, error) {
	result := vectorMigrateArgs{
		BatchSize: 1000,
	}

	for i := 2; i < len(args); i++ {
		switch args[i] {
		case "--qdrant-addr":
			if i+1 >= len(args) {
				return result, fmt.Errorf("migrate-vectors: --qdrant-addr requires a value")
			}
			i++
			result.QdrantAddr = args[i]
		case "--batch-size":
			if i+1 >= len(args) {
				return result, fmt.Errorf("migrate-vectors: --batch-size requires a value")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				return result, fmt.Errorf("migrate-vectors: --batch-size must be a positive integer")
			}
			result.BatchSize = n
		case "--dry-run":
			result.DryRun = true
		default:
			return result, fmt.Errorf("migrate-vectors: unknown flag %q", args[i])
		}
	}

	if result.QdrantAddr == "" {
		return result, fmt.Errorf("migrate-vectors: --qdrant-addr is required\nusage: nram migrate-vectors --qdrant-addr <host:port> [--batch-size N] [--dry-run]")
	}

	return result, nil
}

// parseEmbeddingText parses a pgvector text-format embedding string into a []float32.
// The expected format is "[0.1,0.2,0.3]".
func parseEmbeddingText(s string) ([]float32, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf("migrate-vectors: invalid embedding format: expected [...]")
	}

	inner := s[1 : len(s)-1]
	if inner == "" {
		return []float32{}, nil
	}

	parts := strings.Split(inner, ",")
	result := make([]float32, len(parts))
	for i, p := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("migrate-vectors: invalid float at index %d: %w", i, err)
		}
		result[i] = float32(v)
	}

	return result, nil
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

// qdrantCollectionName returns the collection name for a given dimension.
func qdrantCollectionName(dim int) string {
	return fmt.Sprintf("nram_vectors_%d", dim)
}

// handleMigrateVectors migrates vectors from PostgreSQL pgvector tables to Qdrant.
func handleMigrateVectors(args []string, db *sql.DB) error {
	parsed, err := parseVectorMigrateArgs(args)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Verify the source is Postgres by attempting to query a pgvector table.
	var check int
	err = db.QueryRowContext(ctx, "SELECT 1 FROM information_schema.tables WHERE table_name = 'memory_vectors_384' LIMIT 1").Scan(&check)
	if err != nil {
		return fmt.Errorf("migrate-vectors: source database does not appear to have pgvector tables (is it Postgres?): %w", err)
	}

	// Connect to Qdrant.
	host, port, err := parseQdrantAddr(parsed.QdrantAddr)
	if err != nil {
		return fmt.Errorf("migrate-vectors: invalid Qdrant address %q: %w", parsed.QdrantAddr, err)
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})
	if err != nil {
		return fmt.Errorf("migrate-vectors: failed to connect to Qdrant at %s: %w", parsed.QdrantAddr, err)
	}
	defer client.Close()

	// Ensure collections exist.
	for _, dim := range vectorDimensions {
		name := qdrantCollectionName(dim)
		exists, err := client.CollectionExists(ctx, name)
		if err != nil {
			return fmt.Errorf("migrate-vectors: failed to check collection %s: %w", name, err)
		}
		if exists {
			continue
		}

		err = client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: name,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     uint64(dim),
				Distance: qdrant.Distance_Cosine,
			}),
		})
		if err != nil {
			return fmt.Errorf("migrate-vectors: failed to create collection %s: %w", name, err)
		}

		_, err = client.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
			CollectionName: name,
			FieldName:      "namespace_id",
			FieldType:      qdrant.PtrOf(qdrant.FieldType_FieldTypeKeyword),
		})
		if err != nil {
			return fmt.Errorf("migrate-vectors: failed to create namespace_id index on %s: %w", name, err)
		}
	}

	fmt.Printf("connected to Qdrant at %s\n", parsed.QdrantAddr)

	totalMigrated := 0

	for _, dim := range vectorDimensions {
		table := qdrantCollectionName(dim)
		pgTable := fmt.Sprintf("memory_vectors_%d", dim)
		count, err := migrateVectorTable(ctx, db, client, pgTable, table, dim, parsed.BatchSize, parsed.DryRun)
		if err != nil {
			return fmt.Errorf("migrate-vectors: failed to migrate %s: %w", pgTable, err)
		}
		if count > 0 {
			if parsed.DryRun {
				fmt.Printf("  [dry-run] would migrate %d vectors from %s\n", count, pgTable)
			} else {
				fmt.Printf("  migrated %d vectors from %s\n", count, pgTable)
			}
		} else {
			fmt.Printf("  no vectors in %s\n", pgTable)
		}
		totalMigrated += count
	}

	if parsed.DryRun {
		fmt.Printf("dry run complete: %d total vectors would be migrated\n", totalMigrated)
	} else {
		fmt.Printf("migration complete: %d total vectors migrated to Qdrant\n", totalMigrated)
	}

	return nil
}

// migrateVectorTable reads vectors from a single pgvector dimension table and upserts them into Qdrant.
// Returns the number of vectors processed.
func migrateVectorTable(ctx context.Context, db *sql.DB, client *qdrant.Client, pgTable string, collection string, dimension int, batchSize int, dryRun bool) (int, error) {
	query := fmt.Sprintf(
		`SELECT v.memory_id, m.namespace_id, v.embedding::text FROM %s v JOIN memories m ON v.memory_id = m.id`,
		pgTable,
	)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	total := 0
	batch := make([]*qdrant.PointStruct, 0, batchSize)

	for rows.Next() {
		var memoryIDStr, namespaceIDStr, embeddingText string
		if err := rows.Scan(&memoryIDStr, &namespaceIDStr, &embeddingText); err != nil {
			return total, fmt.Errorf("scan failed: %w", err)
		}

		if _, err := uuid.Parse(memoryIDStr); err != nil {
			return total, fmt.Errorf("invalid memory_id %q: %w", memoryIDStr, err)
		}

		if _, err := uuid.Parse(namespaceIDStr); err != nil {
			return total, fmt.Errorf("invalid namespace_id %q: %w", namespaceIDStr, err)
		}

		embedding, err := parseEmbeddingText(embeddingText)
		if err != nil {
			return total, fmt.Errorf("failed to parse embedding for memory %s: %w", memoryIDStr, err)
		}

		total++

		if dryRun {
			continue
		}

		batch = append(batch, &qdrant.PointStruct{
			Id:      qdrant.NewID(memoryIDStr),
			Vectors: qdrant.NewVectorsDense(embedding),
			Payload: qdrant.NewValueMap(map[string]any{
				"namespace_id": namespaceIDStr,
			}),
		})

		if len(batch) >= batchSize {
			_, err := client.Upsert(ctx, &qdrant.UpsertPoints{
				CollectionName: collection,
				Points:         batch,
			})
			if err != nil {
				return total, fmt.Errorf("batch upsert failed: %w", err)
			}
			batch = batch[:0]
		}
	}

	if err := rows.Err(); err != nil {
		return total, fmt.Errorf("rows iteration error: %w", err)
	}

	// Flush remaining batch.
	if !dryRun && len(batch) > 0 {
		_, err := client.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: collection,
			Points:         batch,
		})
		if err != nil {
			return total, fmt.Errorf("final batch upsert failed: %w", err)
		}
	}

	return total, nil
}
