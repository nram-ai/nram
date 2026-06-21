package service_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/storage"
)

// allowedFacetProbeDims mirrors the vector-table dims provisioned by migration
// 000006/000057 (memory_vectors_<dim>). The probe templates the table name by
// string substitution, so the dim is constrained to this set to keep it a fixed
// identifier rather than anything derived from free-form input.
var allowedFacetProbeDims = map[string]bool{
	"384": true, "512": true, "768": true, "1024": true, "1536": true, "3072": true,
}

// TestFacetRecovery_LiveData verifies that multi-vector topic facets recover the
// sub-topics that a single pooled vector dilutes on consolidated (dream-synthesis)
// memories. Under one pooled vector a multi-topic synthesis represents each of its
// individual source memories poorly; topic facets (facet 0 = pooled,
// facets 1..N = topic facets) are meant to fix that. The probe compares, per
// synthesis, the worst-covered source member's cosine under the pooled vector alone
// ("before") against the max over all of the synthesis's facets ("after"). If
// facets recover the diluted sub-topics, the "after" metrics rise and the
// multi-topic fraction falls.
//
// The probe is a single SELECT (testdata/facet_recovery/probe.sql) run inside a
// read-only transaction; it never writes to the database.
//
// Skipped unless FACET_PROBE_DATABASE_URL points at the target backend.
// FACET_PROBE_DIM overrides the embedding dim (default 1024). Usage:
//
//	FACET_PROBE_DATABASE_URL='postgres://user:pass@host/db?sslmode=disable' \
//	  go test ./internal/service/ -run TestFacetRecovery_LiveData -count=1 -v -timeout=300s
func TestFacetRecovery_LiveData(t *testing.T) {
	url := os.Getenv("FACET_PROBE_DATABASE_URL")
	if url == "" {
		t.Skip("FACET_PROBE_DATABASE_URL not set; skipping live facet recovery probe")
	}
	dim := os.Getenv("FACET_PROBE_DIM")
	if dim == "" {
		dim = "1024"
	}
	if !allowedFacetProbeDims[dim] {
		t.Fatalf("FACET_PROBE_DIM=%q is not a provisioned vector dim", dim)
	}

	sqlBytes, err := os.ReadFile("testdata/facet_recovery/probe.sql")
	if err != nil {
		t.Fatalf("read probe.sql: %v", err)
	}
	probe := string(sqlBytes)
	if dim != "1024" {
		probe = strings.ReplaceAll(probe, "memory_vectors_1024", "memory_vectors_"+dim)
		probe = strings.ReplaceAll(probe, "embedding_dim = 1024", "embedding_dim = "+dim)
	}

	ctx := context.Background()
	db, err := storage.Open(config.DatabaseConfig{URL: url})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if db.Backend() != "postgres" {
		t.Skipf("facet recovery probe targets postgres; backend is %s", db.Backend())
	}

	// A genuine read-only transaction: pgx issues SET TRANSACTION READ ONLY, so any
	// accidental write inside the probe errors out instead of mutating the corpus.
	conn, err := db.DB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin read-only tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		syntheses                                            int
		avgSources                                           float64
		medBefore, medAfter, p10Before, p10After             float64
		multiBefore, multiAfter, below60Before, below60After float64
		tightBefore, tightAfter                              float64
	)
	if err := tx.QueryRowContext(ctx, probe).Scan(
		&syntheses, &avgSources,
		&medBefore, &medAfter, &p10Before, &p10After,
		&multiBefore, &multiAfter, &below60Before, &below60After,
		&tightBefore, &tightAfter,
	); err != nil {
		t.Fatalf("run probe: %v", err)
	}

	if syntheses == 0 {
		t.Fatalf("no dream syntheses with >=2 still-vectored sources at dim %s; nothing to measure", dim)
	}

	t.Logf("facet recovery probe (dim %s): %d syntheses, avg %.2f sources", dim, syntheses, avgSources)
	t.Logf("  worst-covered member cosine  median: %.3f -> %.3f   p10: %.3f -> %.3f",
		medBefore, medAfter, p10Before, p10After)
	t.Logf("  multi-topic (min<0.45): %.1f%% -> %.1f%%   below 0.60: %.1f%% -> %.1f%%   tight (min>=0.75): %.1f%% -> %.1f%%",
		multiBefore*100, multiAfter*100, below60Before*100, below60After*100, tightBefore*100, tightAfter*100)

	// The recovery property the probe exists to verify: facets must not regress the
	// worst-covered member and must reduce the multi-topic (diluted) fraction.
	if medAfter < medBefore {
		t.Errorf("max-over-facets regressed the median worst-covered member: %.3f -> %.3f", medBefore, medAfter)
	}
	if p10After < p10Before {
		t.Errorf("max-over-facets regressed the p10 worst-covered member: %.3f -> %.3f", p10Before, p10After)
	}
	if multiAfter > multiBefore {
		t.Errorf("max-over-facets increased the multi-topic fraction: %.3f -> %.3f", multiBefore, multiAfter)
	}
}
