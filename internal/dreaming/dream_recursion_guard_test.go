package dreaming

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// TestDreamRecursionGuard_EndToEnd is the behavioral enforcer of the dream-
// of-dream-of-dream cascade prevention contract spelled out in cross-
// referenced comments at:
//
//   - internal/dreaming/phase_consolidation.go (synthMemory creation;
//       "DREAM-RECURSION GUARD — first prong"; sets Source=DreamSource and
//       Enriched=true)
//   - internal/dreaming/phase_consolidation.go (consolidate() candidate
//       filter; "DREAM-RECURSION GUARD — second prong")
//   - internal/enrichment/worker.go (WorkerPool.runPreEmbed skipFact /
//       skipEntity gate)
//   - internal/enrichment/phase_ingestion.go (runIngestionDecision
//       Enriched/source early-return)
//
// The contract: a dream-source memory enrolled in enrichment MUST NOT
// produce any derivative rows that the next dream cycle could re-cluster:
//
//   - zero extracted_fact lineage children
//   - zero entity upserts sourced from the dream memory
//   - zero relationship rows sourced from the dream memory
//
// Embedding and the Enriched-flag mark all run normally so the dream
// memory becomes vector-searchable.
//
// The test wires the REAL EnrichmentQueueRepo + MemoryRepo + LineageRepo +
// EntityRepo + RelationshipRepo against an in-memory SQLite DB, enqueues a
// dream-source memory, runs the worker pool until idle, and asserts the
// contract holds on the actual database rows.
//
// To fail loudly when the guard breaks, the mock fact and entity LLM
// providers return VALID extraction JSON if called — so a dropped guard
// produces real extracted-fact children that show up in the lineage table
// and trip the assertions. The mock providers also record their call
// counts so a partial failure (one guard removed, the other still
// standing) is diagnosable from the test output.
//
// The test is table-driven across two seed shapes — Enriched=true (the
// production path that ConsolidationPhase produces) and Enriched=false
// (synthetic case that proves the source check is independently load-
// bearing). If only one clause were sufficient, removing the OTHER one
// would silently pass; running both sub-cases pins each clause as load-
// bearing on its own.
func TestDreamRecursionGuard_EndToEnd(t *testing.T) {
	cases := []struct {
		name     string
		enriched bool
	}{
		// Production shape: ConsolidationPhase sets Enriched=true at
		// synthesis-creation time. Both guards (source check and the
		// Enriched flag) are present, so removing either alone still
		// passes this sub-test — the COMBINATION is what protects
		// production. The Enriched=false case below pins the source
		// check on its own.
		{"enriched=true (production shape)", true},
		// Synthetic case: a dream memory with Enriched=false isolates
		// the source==DreamSource clause. If that clause were removed
		// from worker.go skipFact/skipEntity or from phase_ingestion.go
		// early-return, this sub-test would proceed to fact extraction
		// and fail the assertions below.
		{"enriched=false (source-check only)", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runRecursionGuardCase(t, tc.enriched)
		})
	}
}

func runRecursionGuardCase(t *testing.T, enriched bool) {
	ctx := context.Background()
	db := setupRecursionGuardDB(t)

	// Real namespace so foreign keys resolve. No project is required for
	// the bare worker path under test (cascade resolver is nil).
	ns := &model.Namespace{
		ID:        uuid.New(),
		Kind:      "project",
		CreatedAt: time.Now().UTC(),
	}
	nsRepo := storage.NewNamespaceRepo(db)
	if err := nsRepo.Create(ctx, ns); err != nil {
		t.Fatalf("namespace create: %v", err)
	}

	// Real repos for every table the worker reads or writes.
	memoryRepo := storage.NewMemoryRepo(db)
	lineageRepo := storage.NewMemoryLineageRepo(db)
	entityRepo := storage.NewEntityRepo(db)
	relRepo := storage.NewRelationshipRepo(db)
	queueRepo := storage.NewEnrichmentQueueRepo(db)

	// Seed a dream-source synthesis memory. The enriched flag is varied
	// across sub-cases to pin each clause of the skip predicate
	// independently.
	source := model.DreamSource
	dreamMem := &model.Memory{
		ID:          uuid.New(),
		NamespaceID: ns.ID,
		Content:     "Synthesized dream content. Alice works at Acme.",
		Source:      &source,
		Confidence:  0.5,
		Importance:  0.5,
		Enriched:    enriched,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	if err := memoryRepo.Create(ctx, dreamMem); err != nil {
		t.Fatalf("seed dream memory: %v", err)
	}

	// Enqueue via the real repo so the worker claims it through the same
	// path production uses.
	now := time.Now().UTC()
	job := &model.EnrichmentJob{
		ID:          uuid.New(),
		MemoryID:    dreamMem.ID,
		NamespaceID: ns.ID,
		Status:      model.EnrichmentStatusPending,
		Priority:    0,
		MaxAttempts: 3,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := queueRepo.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	// Scripted mocks. Any call into fact or entity providers is a contract
	// violation — record it and use the recorded count to diagnose which
	// guard slipped. The response payloads are real-shaped JSON so that, if
	// the guard breaks, the worker proceeds to write extracted_fact
	// children rather than failing earlier on a parse error.
	var factCalls, entityCalls, embedCalls atomic.Int64

	factLLM := &recursionGuardLLM{
		name:    "guard-fact-mock",
		counter: &factCalls,
		body:    `{"facts":[{"content":"Alice works at Acme.","tags":[]}]}`,
	}
	entityLLM := &recursionGuardLLM{
		name:    "guard-entity-mock",
		counter: &entityCalls,
		body:    `{"entities":[{"name":"Alice","kind":"person"}],"relationships":[]}`,
	}
	embed := &recursionGuardEmbed{
		name:    "guard-embed-mock",
		counter: &embedCalls,
		dim:     384,
	}

	// Vector store is a recording mock that already exists in this
	// package for the embedding-backfill tests; reusing it keeps the
	// VectorStore double surface minimal.
	vectorStore := &recordingVectorStore{}

	// Settings: defaults are fine; the worker's ingestion-decision phase
	// is off by default at the global key. Augmentation defaults to off
	// too; the test still verifies that fact/entity extraction is skipped
	// (the contract).
	settingsSvc := service.NewNoopSettingsService()

	pool := enrichment.NewWorkerPool(
		enrichment.WorkerConfig{Workers: 1, PollInterval: 5 * time.Millisecond},
		memoryRepo, memoryRepo, memoryRepo, memoryRepo, queueRepo,
		entityRepo, relRepo, lineageRepo, vectorStore,
		func() provider.LLMProvider { return factLLM },
		func() provider.LLMProvider { return entityLLM },
		func() provider.EmbeddingProvider { return embed },
		func() provider.LLMProvider { return nil }, // ingestion-decision provider unused
		nil, // deduplicator unused when ingestion-decision is off
		settingsSvc,
		nil, // cascade resolver — nil means "use settings only"
		nil, // event bus
	)

	pool.Start()
	defer pool.Stop()

	// Wait for the pool to fully drain the queue. IsIdle reports when
	// every worker is sleeping; combined with a queue-empty check this
	// gives a stable "the job has been fully processed" signal.
	if err := waitUntilDrained(ctx, pool, queueRepo, 5*time.Second); err != nil {
		t.Fatalf("worker pool did not drain: %v", err)
	}

	// --- Assertions: every contract guarantee ---

	// 1. Fact LLM was never invoked.
	if calls := factCalls.Load(); calls != 0 {
		t.Errorf("fact extraction LLM was called %d time(s) for a dream memory; contract violated. Likely cause: skipFact predicate in enrichment/worker.go lost its isDream clause.", calls)
	}

	// 2. Entity LLM was never invoked.
	if calls := entityCalls.Load(); calls != 0 {
		t.Errorf("entity extraction LLM was called %d time(s) for a dream memory; contract violated. Likely cause: skipEntity predicate in enrichment/worker.go lost its isDream clause.", calls)
	}

	// 3. No extracted_fact lineage children point at the dream memory.
	hasFactChildren, err := lineageRepo.HasExtractedFactChildren(ctx, ns.ID, dreamMem.ID)
	if err != nil {
		t.Fatalf("query extracted-fact lineage: %v", err)
	}
	if hasFactChildren {
		t.Errorf("memory_lineage contains extracted_fact rows whose parent is the dream memory; contract violated.")
	}

	// 4. No new entity rows were created in the namespace.
	entities, err := entityRepo.ListByNamespace(ctx, ns.ID)
	if err != nil {
		t.Fatalf("list entities: %v", err)
	}
	if len(entities) != 0 {
		names := make([]string, 0, len(entities))
		for _, e := range entities {
			names = append(names, e.Name)
		}
		t.Errorf("entity table is not empty after processing a dream memory; got %d entities: %v. Contract violated: entity extraction must skip.", len(entities), names)
	}

	// 5. No relationship rows sourced from the dream memory.
	hasRels, err := relRepo.HasBySourceMemory(ctx, ns.ID, dreamMem.ID)
	if err != nil {
		t.Fatalf("query relationships: %v", err)
	}
	if hasRels {
		t.Errorf("relationship table has rows sourced from the dream memory; contract violated.")
	}

	// 6. Sanity: the dream memory got embedded. Either embedding_dim is
	//    stamped on the row OR the vector store saw an Upsert for the
	//    dream memory's ID. Without this assertion, a future change that
	//    over-broadens the skip predicate to embedding (silently breaking
	//    dream-memory recall) would not fail this test.
	reloaded, err := memoryRepo.GetByID(ctx, dreamMem.ID)
	if err != nil {
		t.Fatalf("reload dream memory: %v", err)
	}
	upsertSeen := false
	for _, rec := range vectorStore.upserts {
		if rec.ID == dreamMem.ID {
			upsertSeen = true
			break
		}
	}
	if reloaded.EmbeddingDim == nil && !upsertSeen {
		t.Errorf("dream memory did not receive an embedding (embedding_dim=nil and no vector upsert seen); the embedding path must still run for dream memories. embed_calls=%d", embedCalls.Load())
	}
}

// --- support harness ---

func setupRecursionGuardDB(t *testing.T) storage.DB {
	t.Helper()
	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return db
}

// waitUntilDrained blocks until the worker pool reports idle AND the
// enrichment queue has no pending jobs left, or returns an error on
// timeout. Polling at the pool's PollInterval would be wasteful — we
// poll faster here so the test stays snappy.
func waitUntilDrained(ctx context.Context, pool *enrichment.WorkerPool, queue *storage.EnrichmentQueueRepo, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	pendingCount := func() (int, error) {
		stats, err := queue.CountByStatus(ctx)
		if err != nil {
			return 0, err
		}
		return stats.Pending + stats.Processing, nil
	}
	for {
		idle := pool.IsIdle()
		pending, err := pendingCount()
		if err != nil {
			return err
		}
		if idle && pending == 0 {
			// Brief settling: IsIdle can flip true between batches even
			// if a follow-on job is enqueued mid-claim. Pause briefly,
			// then re-check.
			time.Sleep(20 * time.Millisecond)
			if pool.IsIdle() {
				p2, err := pendingCount()
				if err != nil {
					return err
				}
				if p2 == 0 {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return errors.New("worker pool did not reach drained state before timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// recursionGuardLLM returns a scripted body on every Complete call and
// increments its counter. The body is real-shaped so that, if the guard
// breaks, the worker proceeds to actually write extracted-fact children
// (and the test reports the contract violation rather than failing
// earlier on a parse error).
type recursionGuardLLM struct {
	name    string
	counter *atomic.Int64
	body    string
}

func (m *recursionGuardLLM) Name() string     { return m.name }
func (m *recursionGuardLLM) Models() []string { return []string{m.name} }
func (m *recursionGuardLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	m.counter.Add(1)
	return &provider.CompletionResponse{
		Content: m.body,
		Model:   m.name,
		Usage:   provider.TokenUsage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30},
	}, nil
}

// recursionGuardEmbed returns a fixed-dim embedding for every input so
// the worker's embedding step lands a vector and the sanity assertion
// holds.
type recursionGuardEmbed struct {
	name    string
	counter *atomic.Int64
	dim     int
}

func (e *recursionGuardEmbed) Name() string      { return e.name }
func (e *recursionGuardEmbed) Dimensions() []int { return []int{e.dim} }
func (e *recursionGuardEmbed) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	e.counter.Add(1)
	if len(req.Input) == 0 {
		return nil, errors.New("recursionGuardEmbed: empty input")
	}
	embs := make([][]float32, len(req.Input))
	for i := range req.Input {
		vec := make([]float32, e.dim)
		for j := range vec {
			vec[j] = 0.1
		}
		embs[i] = vec
	}
	return &provider.EmbeddingResponse{
		Embeddings: embs,
		Model:      e.name,
		Usage:      provider.TokenUsage{PromptTokens: len(req.Input), TotalTokens: len(req.Input)},
	}, nil
}
