package admin

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// ollamaEmbed fetches a real embedding from the local Ollama server. The test
// is skipped (not failed) when Ollama is unreachable so the suite stays green
// in environments without it.
func ollamaEmbed(t *testing.T, model, text string) []float32 {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"model": model, "prompt": text})
	req, err := http.NewRequest(http.MethodPost, "http://localhost:11434/api/embeddings", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build ollama request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("ollama not reachable: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("ollama /api/embeddings returned %d", resp.StatusCode)
	}
	var out struct {
		Embedding []float64 `json:"embedding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode ollama response: %v", err)
	}
	if len(out.Embedding) == 0 {
		t.Skip("ollama returned an empty embedding")
	}
	v := make([]float32, len(out.Embedding))
	for i, f := range out.Embedding {
		v[i] = float32(f)
	}
	return v
}

// truncateAllPg empties every nram table in the target so the migrator's exact
// row-count validation starts from a clean slate on repeated runs.
func truncateAllPg(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`)
	if err != nil {
		t.Fatalf("list target tables: %v", err)
	}
	var tables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		tables = append(tables, n)
	}
	_ = rows.Close()
	for _, tb := range tables {
		if _, err := db.Exec("TRUNCATE TABLE " + tb + " CASCADE"); err != nil {
			t.Fatalf("truncate %s: %v", tb, err)
		}
	}
}

// parsePgVector parses pgvector's text form "[f1,f2,...]" into a slice.
func parsePgVector(t *testing.T, s string) []float32 {
	t.Helper()
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	parts := strings.Split(s, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			t.Fatalf("parse vector element %q: %v", p, err)
		}
		out[i] = float32(f)
	}
	return out
}

// TestDataMigrator_VectorsPgvector exercises the memory_vectors and
// entity_vectors dimension-sharding paths end-to-end against a real
// pgvector-enabled Postgres, using a genuine embedding from the local Ollama
// model. Gated on NRAM_PGVECTOR_TEST_DSN, which must point at a database where
// `CREATE EXTENSION vector` has already been run (so migration 000007 can build
// the vector(N) tables that are the migration destinations).
func TestDataMigrator_VectorsPgvector(t *testing.T) {
	dsn := os.Getenv("NRAM_PGVECTOR_TEST_DSN")
	if dsn == "" {
		t.Skip("set NRAM_PGVECTOR_TEST_DSN (a pgvector-enabled DB) to run")
	}
	ctx := context.Background()

	emb := ollamaEmbed(t, "nomic-embed-text", "the quick brown fox jumps over the lazy dog")
	dim := len(emb)
	if _, ok := supportedVectorDimensions[dim]; !ok {
		t.Skipf("ollama embedding dim %d is not in the supported set", dim)
	}
	blob := hnsw.EncodeVector(emb)
	t.Logf("got a real %d-dim embedding from ollama nomic-embed-text", dim)

	// ── Source SQLite: a memory and an entity, each carrying the real vector ──
	src := openSQLiteInMemory(t)
	defer func() { _ = src.Close() }()
	mustExec := func(q string, args ...any) {
		if _, err := src.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	nsID := "a0000000-0000-0000-0000-0000000000aa"
	mustExec(`INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth)
		VALUES (?, 'VecOrg', 'vecorg', 'organization', '00000000-0000-0000-0000-000000000000', 'vecorg', 1)`, nsID)
	memID := "b0000000-0000-0000-0000-0000000000bb"
	mustExec(`INSERT INTO memories (id, namespace_id, content, embedding_dim) VALUES (?, ?, 'hello vector world', ?)`, memID, nsID, dim)
	mustExec(`INSERT INTO memory_vectors (memory_id, namespace_id, dimension, embedding) VALUES (?, ?, ?, ?)`, memID, nsID, dim, blob)
	entID := "c0000000-0000-0000-0000-0000000000cc"
	mustExec(`INSERT INTO entities (id, namespace_id, name, canonical, entity_type, embedding_dim) VALUES (?, ?, 'Acme', 'acme', 'organization', ?)`, entID, nsID, dim)
	mustExec(`INSERT INTO entity_vectors (entity_id, namespace_id, dimension, embedding) VALUES (?, ?, ?, ?)`, entID, nsID, dim, blob)

	// ── Clean the target so exact count validation starts fresh ──
	cleanConn, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open target for cleanup: %v", err)
	}
	truncateAllPg(t, cleanConn)
	_ = cleanConn.Close()

	// ── Migrate (newDataMigrator runs schema migrations on the target, which
	// now succeed because the vector extension exists) and copy the data ──
	dm, err := newDataMigrator(ctx, src, dsn)
	if err != nil {
		t.Fatalf("newDataMigrator: %v", err)
	}
	defer func() { _ = dm.Close() }()
	if err := dm.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ── Verify both sharded vector tables round-trip the real embedding ──
	pg, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open target for verify: %v", err)
	}
	defer func() { _ = pg.Close() }()

	memTable := fmt.Sprintf("memory_vectors_%d", dim)
	entTable := fmt.Sprintf("entity_vectors_%d", dim)

	check := func(table, idCol, id string) {
		var got string
		if err := pg.QueryRowContext(ctx,
			fmt.Sprintf("SELECT embedding::text FROM %s WHERE %s = $1", table, idCol), id,
		).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", table, err)
		}
		vec := parsePgVector(t, got)
		if len(vec) != dim {
			t.Fatalf("%s: dim = %d, want %d", table, len(vec), dim)
		}
		for i := range emb {
			if d := math.Abs(float64(vec[i] - emb[i])); d > 1e-4 {
				t.Fatalf("%s element %d: got %v want %v (delta %v)", table, i, vec[i], emb[i], d)
			}
		}
		t.Logf("OK: %s holds the real %d-dim embedding, values match within 1e-4", table, dim)
	}

	check(memTable, "memory_id", memID)
	check(entTable, "entity_id", entID)
}
