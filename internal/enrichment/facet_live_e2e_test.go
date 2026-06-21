package enrichment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// liveOllamaEmbedder is a minimal EmbeddingProvider that calls a real Ollama
// /api/embed endpoint, so the live e2e exercises actual embeddings rather than a
// fake. Self-contained on purpose (no dependency on the provider registry's slot
// wiring) so the test is a stable artifact.
type liveOllamaEmbedder struct {
	url, model string
	dim        int
}

func (e *liveOllamaEmbedder) Name() string      { return "ollama-live" }
func (e *liveOllamaEmbedder) Dimensions() []int { return []int{e.dim} }
func (e *liveOllamaEmbedder) Embed(ctx context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	body, _ := json.Marshal(map[string]any{"model": e.model, "input": req.Input})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama embed status %d", resp.StatusCode)
	}
	var out struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &provider.EmbeddingResponse{Embeddings: out.Embeddings}, nil
}

// TestLiveE2E_FacetsImproveSubtopicRecall is a real-stack end-to-end: it embeds a
// genuinely two-topic memory with the local qwen3-embedding model, extracts
// facets, writes them to both live vector backends (pgvector on this box, Qdrant
// on .43), and asserts that a query about the memory's minor sub-topic retrieves
// it more strongly via its topic facet than via the pooled whole-memory vector.
//
// Gated on NRAM_LIVE_E2E. Endpoints come from env with sensible local defaults:
//
//	NRAM_LIVE_E2E=1
//	OLLAMA_URL (default http://localhost:11434)
//	PGVECTOR_TEST_DSN (required for the pgvector leg)
//	QDRANT_TEST_ADDR (default 192.168.2.43:6334)
func TestLiveE2E_FacetsImproveSubtopicRecall(t *testing.T) {
	if os.Getenv("NRAM_LIVE_E2E") == "" {
		t.Skip("set NRAM_LIVE_E2E to run the live real-model e2e")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ollamaURL := envOr("OLLAMA_URL", "http://localhost:11434")
	dim := 1024
	emb := &liveOllamaEmbedder{url: ollamaURL, model: "qwen3-embedding:0.6b", dim: dim}

	// Two clearly distinct topics so real embeddings cluster into two facets.
	content := "Water boils at one hundred degrees Celsius at sea level. " +
		"Heating a liquid raises its molecules' kinetic energy until it vaporizes into steam. " +
		"Medieval castles relied on thick stone curtain walls for defense. " +
		"A moat and a raised drawbridge guarded the castle gatehouse against attackers."
	subTopicQuery := "How did fortified castles defend their gates in the middle ages?"

	wr, err := emb.Embed(ctx, &provider.EmbeddingRequest{Input: []string{content}, Dimension: dim})
	if err != nil {
		t.Fatalf("embed whole content: %v", err)
	}
	whole := wr.Embeddings[0]
	if len(whole) != dim {
		t.Fatalf("expected %d-dim embedding, got %d (is qwen3-embedding:0.6b the active model?)", dim, len(whole))
	}

	facets, err := ExtractFacets(ctx, emb, content, whole, dim, 0.65, 8)
	if err != nil {
		t.Fatalf("ExtractFacets: %v", err)
	}
	t.Logf("real qwen3-embedding produced %d facets (facet 0 + %d topic facets)", len(facets), len(facets)-1)
	if len(facets) < 2 {
		t.Fatalf("expected the two-topic memory to yield >=2 facets, got %d", len(facets))
	}

	qr, err := emb.Embed(ctx, &provider.EmbeddingRequest{Input: []string{subTopicQuery}, Dimension: dim})
	if err != nil {
		t.Fatalf("embed query: %v", err)
	}
	query := qr.Embeddings[0]

	t.Run("pgvector", func(t *testing.T) {
		dsn := os.Getenv("PGVECTOR_TEST_DSN")
		if dsn == "" {
			t.Skip("set PGVECTOR_TEST_DSN for the pgvector leg")
		}
		pooled, faceted := runPgvectorFacetE2E(ctx, t, dsn, dim, whole, facets, query)
		assertFacetWins(t, "pgvector", pooled, faceted)
	})

	t.Run("qdrant", func(t *testing.T) {
		addr := envOr("QDRANT_TEST_ADDR", "192.168.2.43:6334")
		store, err := storage.NewQdrantStore(storage.QdrantConfig{Addr: addr})
		if err != nil {
			t.Fatalf("NewQdrantStore: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := store.EnsureCollections(ctx); err != nil {
			t.Fatalf("EnsureCollections: %v", err)
		}
		ns := uuid.New()
		pooledMem, facetMem := uuid.New(), uuid.New()
		t.Cleanup(func() {
			_ = store.Delete(ctx, storage.VectorKindMemory, pooledMem)
			_ = store.Delete(ctx, storage.VectorKindMemory, facetMem)
		})
		if err := store.Upsert(ctx, storage.VectorKindMemory, pooledMem, ns, whole, dim); err != nil {
			t.Fatalf("qdrant upsert pooled: %v", err)
		}
		if err := store.UpsertFacets(ctx, facetMem, ns, dim, facets); err != nil {
			t.Fatalf("qdrant upsert facets: %v", err)
		}
		pooled := scoreFor(t, ctx, store, ns, dim, query, pooledMem)
		faceted := scoreFor(t, ctx, store, ns, dim, query, facetMem)
		assertFacetWins(t, "qdrant", pooled, faceted)
	})
}

// runPgvectorFacetE2E provisions a faceted pgvector schema, writes a pooled-only
// memory and a faceted memory, and returns each one's sub-topic-query score.
func runPgvectorFacetE2E(ctx context.Context, t *testing.T, dsn string, dim int, whole []float32, facets [][]float32, query []float32) (pooled, faceted float64) {
	t.Helper()
	// DDL via a plain connection (vector type comes from the extension; the Go
	// pgvector type registration is only needed for the store's encode path).
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	schema := `
		CREATE EXTENSION IF NOT EXISTS vector;
		CREATE TABLE IF NOT EXISTS namespaces (id UUID PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS memories (id UUID PRIMARY KEY, namespace_id UUID NOT NULL REFERENCES namespaces(id), content TEXT NOT NULL DEFAULT '', deleted_at TIMESTAMPTZ);
		CREATE TABLE IF NOT EXISTS memory_vectors_1024 (
			memory_id UUID REFERENCES memories(id) ON DELETE CASCADE,
			facet_id SMALLINT NOT NULL DEFAULT 0,
			embedding vector(1024) NOT NULL,
			PRIMARY KEY (memory_id, facet_id)
		);`
	if _, err := conn.Exec(ctx, schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	ns := uuid.New()
	pooledMem, facetMem := uuid.New(), uuid.New()
	if _, err := conn.Exec(ctx, "INSERT INTO namespaces (id, name) VALUES ($1, 'e2e')", ns); err != nil {
		t.Fatalf("insert ns: %v", err)
	}
	for _, id := range []uuid.UUID{pooledMem, facetMem} {
		if _, err := conn.Exec(ctx, "INSERT INTO memories (id, namespace_id, content) VALUES ($1, $2, 'm')", id, ns); err != nil {
			t.Fatalf("insert memory: %v", err)
		}
	}
	t.Cleanup(func() {
		c, e := pgx.Connect(context.Background(), dsn)
		if e == nil {
			_, _ = c.Exec(context.Background(), "DELETE FROM memory_vectors_1024 WHERE memory_id = ANY($1)", []uuid.UUID{pooledMem, facetMem})
			_, _ = c.Exec(context.Background(), "DELETE FROM memories WHERE id = ANY($1)", []uuid.UUID{pooledMem, facetMem})
			_, _ = c.Exec(context.Background(), "DELETE FROM namespaces WHERE id = $1", ns)
			_ = c.Close(context.Background())
		}
	})

	store, err := storage.NewPgVectorStore(dsn)
	if err != nil {
		t.Fatalf("NewPgVectorStore: %v", err)
	}
	defer store.Close()
	if err := store.Upsert(ctx, storage.VectorKindMemory, pooledMem, ns, whole, dim); err != nil {
		t.Fatalf("pgvector upsert pooled: %v", err)
	}
	if err := store.UpsertFacets(ctx, facetMem, ns, dim, facets); err != nil {
		t.Fatalf("pgvector upsert facets: %v", err)
	}
	return scoreFor(t, ctx, store, ns, dim, query, pooledMem), scoreFor(t, ctx, store, ns, dim, query, facetMem)
}

// scoreFor runs a Search and returns the score of the given memory (0 if absent).
func scoreFor(t *testing.T, ctx context.Context, store storage.VectorStore, ns uuid.UUID, dim int, query []float32, id uuid.UUID) float64 {
	t.Helper()
	results, err := store.Search(ctx, storage.VectorKindMemory, query, ns, dim, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	for _, r := range results {
		if r.ID == id {
			return r.Score
		}
	}
	return 0
}

func assertFacetWins(t *testing.T, backend string, pooled, faceted float64) {
	t.Helper()
	t.Logf("%s: sub-topic score pooled=%.3f faceted=%.3f", backend, pooled, faceted)
	if faceted <= pooled {
		t.Errorf("%s: faceted score %.3f did not beat pooled-only %.3f; the topic facet should win on a sub-topic query", backend, faceted, pooled)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
