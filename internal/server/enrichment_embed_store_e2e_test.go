package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/observability/metrics"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
	adminstore "github.com/nram-ai/nram/internal/storage/admin"
)

// TestE2E_EnrichmentEmbedAndStore drives one memory through the full running
// server and asserts that a real embedding vector was persisted.
//
// It closes the coverage gap recorded in the backlog audit: no existing test
// persisted a provider embedding slot and drove a memory through the running
// server (DB persist -> worker claim -> embed -> vector store) to an asserted
// stored vector row. usage_recording_e2e_test.go calls Embed directly (no
// worker, no persistence); enrichment.TestProcessJob_FullPipeline asserts
// against an in-memory mockVectorWriter and bypasses the provider slot/registry
// wiring; facet_live_e2e_test.go is an env-gated direct-embedder probe. This
// test exercises the whole chain, in-process, with zero external dependencies:
//
//   - a fake OpenAI-compatible provider host (httptest) for /v1/models,
//     /v1/chat/completions, and /v1/embeddings,
//   - three persisted provider slots (embedding, fact, entity) built into a
//     real provider registry (so registry.GetEmbedding is a real OpenAIProvider
//     pointed at the fake host — the slot wiring the note says is never tested),
//   - a real enrichment WorkerPool over the SQLite enrichment queue,
//   - the SQLite HNSW vector store,
//   - ingestion driven through the assembled HTTP MCP server.
//
// The assertion reads the persisted vector back through the VectorStore
// interface and checks dimension, non-null contents, and ownership.
func TestE2E_EnrichmentEmbedAndStore(t *testing.T) {
	ctx := context.Background()

	// --- Fake OpenAI-compatible provider host -----------------------------
	const embedDim = 1024
	const servedModel = "fake-embed-model"
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/models":
			writeJSON(t, w, map[string]any{
				"object": "list",
				"data":   []map[string]any{{"id": servedModel}},
			})
		case "POST /v1/chat/completions":
			// Fact and entity extraction both expect a JSON array. Returning an
			// empty array makes both phases succeed with nothing extracted, so
			// the worker proceeds to embed the raw parent memory.
			writeJSON(t, w, map[string]any{
				"id":    "chatcmpl-fake",
				"model": servedModel,
				"choices": []map[string]any{{
					"index":         0,
					"message":       map[string]any{"role": "assistant", "content": "[]"},
					"finish_reason": "stop",
				}},
				"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		case "POST /v1/embeddings":
			var req struct {
				Input []string `json:"input"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			vec := make([]float32, embedDim)
			for i := range vec {
				vec[i] = 0.01 // fixed, non-zero
			}
			data := make([]map[string]any, len(req.Input))
			for i := range req.Input {
				data[i] = map[string]any{"object": "embedding", "index": i, "embedding": vec}
			}
			writeJSON(t, w, map[string]any{
				"object": "list",
				"model":  servedModel,
				"data":   data,
				"usage":  map[string]any{"prompt_tokens": 1, "total_tokens": 1},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)

	// --- DB + user + project ---------------------------------------------
	db := e2eTestDB(t)
	user := e2eTestUser(t, db)

	nsRepo := storage.NewNamespaceRepo(db)
	projectRepo := storage.NewProjectRepo(db)

	// Seed a project under the user's namespace so the store tool can resolve
	// it by slug and persist into its namespace.
	projectNSID := uuid.New()
	projectNS := &model.Namespace{
		ID:       projectNSID,
		Name:     "embed-e2e",
		Slug:     "embed-e2e",
		Kind:     "project",
		ParentID: &user.NamespaceID,
		Path:     "/users/embed-e2e/" + projectNSID.String(),
		Depth:    3,
	}
	if err := nsRepo.Create(ctx, projectNS); err != nil {
		t.Fatalf("create project namespace: %v", err)
	}
	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      projectNSID,
		OwnerNamespaceID: user.NamespaceID,
		Name:             "Embed E2E",
		Slug:             "embed-e2e",
	}
	if err := projectRepo.Create(ctx, project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	// --- Persist provider slots + build the registry from the DB ----------
	settingsRepo := storage.NewSettingsRepo(db)
	slotValue, err := json.Marshal(map[string]any{
		"type":  provider.ProviderTypeOpenAICompatible,
		"url":   fake.URL,
		"model": servedModel,
	})
	if err != nil {
		t.Fatalf("marshal slot config: %v", err)
	}
	for _, slot := range []string{provider.SlotEmbedding, provider.SlotFact, provider.SlotEntity} {
		if err := settingsRepo.Set(ctx, &model.Setting{
			Key:   "provider." + slot,
			Value: slotValue,
			Scope: "global",
		}); err != nil {
			t.Fatalf("persist provider slot %q: %v", slot, err)
		}
	}

	tokenUsageRepo := storage.NewTokenUsageRepo(db)
	regCfg := adminstore.LoadProviderRegistryConfig(ctx, settingsRepo)
	registry, err := provider.NewRegistry(regCfg, tokenUsageRepo, nsRepo)
	if err != nil {
		t.Fatalf("build provider registry: %v", err)
	}
	if registry.GetEmbedding() == nil {
		t.Fatal("registry has no embedding provider after persisting the embedding slot")
	}

	// --- Vector store + repos + worker ------------------------------------
	settingsSvc := service.NewSettingsService(settingsRepo)
	// Resolve the HNSW tuning from the same settings defaults the boot path uses
	// (cmd/server/main.go buildHNSWConfig) so the test tracks production rather
	// than pinning a hand-copied snapshot that can silently drift. The facet gate
	// (SetFacetGate) that boot installs is intentionally left unset: this test
	// stores one non-faceted memory, for which the gate is inert.
	vectorStore := storage.NewHNSWStore(db.DB(), db.WriteDB(), storage.HNSWConfig{
		M:                settingsSvc.ResolveIntWithDefault(ctx, service.SettingHNSWM, "global"),
		EfConstruction:   settingsSvc.ResolveIntWithDefault(ctx, service.SettingHNSWEfConstruction, "global"),
		EfSearch:         settingsSvc.ResolveIntWithDefault(ctx, service.SettingHNSWEfSearch, "global"),
		MaxLoadedIndexes: settingsSvc.ResolveIntWithDefault(ctx, service.SettingHNSWMaxLoadedIndexes, "global"),
	})
	t.Cleanup(func() { _ = vectorStore.Close() })

	memoryRepo := storage.NewMemoryRepo(db)
	enrichmentQueueRepo := storage.NewEnrichmentQueueRepo(db)
	entityRepo := storage.NewEntityRepo(db)
	relationshipRepo := storage.NewRelationshipRepo(db)
	lineageRepo := storage.NewMemoryLineageRepo(db)
	ingestionLogRepo := storage.NewIngestionLogRepo(db)

	llmAccessor := func(get func(*provider.Registry) provider.LLMProvider) func() provider.LLMProvider {
		return func() provider.LLMProvider { return get(registry) }
	}
	factProvider := llmAccessor((*provider.Registry).GetFact)
	entityProvider := llmAccessor((*provider.Registry).GetEntity)
	queryAugmentProvider := llmAccessor((*provider.Registry).GetQueryAugment)
	ingestionProvider := llmAccessor((*provider.Registry).GetIngestionDecision)
	embedProvider := func() provider.EmbeddingProvider { return registry.GetEmbedding() }

	ingestionDedup := enrichment.NewDeduplicator(vectorStore, embedProvider, memoryRepo, enrichment.DefaultDeduplicationConfig)

	workerPool := enrichment.NewWorkerPool(
		enrichment.WorkerConfig{Workers: 1, PollInterval: 10 * time.Millisecond, Backend: db.Backend()},
		memoryRepo, memoryRepo, memoryRepo, memoryRepo, enrichmentQueueRepo,
		entityRepo, relationshipRepo, lineageRepo, vectorStore,
		factProvider, entityProvider, embedProvider,
		ingestionProvider, queryAugmentProvider, ingestionDedup, settingsSvc, nil,
		nil,
	)
	workerPool.Start()
	t.Cleanup(workerPool.Stop)

	// --- Assemble the running server (MCP over the real router) -----------
	storeSvc := service.NewStoreService(memoryRepo, projectRepo, nsRepo, ingestionLogRepo, enrichmentQueueRepo, settingsSvc)
	recallSvc := service.NewRecallService(memoryRepo, projectRepo, nsRepo, vectorStore, entityRepo, relationshipRepo, embedProvider)
	forgetSvc := service.NewForgetService(memoryRepo, projectRepo, vectorStore, lineageRepo)
	updateSvc := service.NewUpdateService(memoryRepo, projectRepo, vectorStore, embedProvider, enrichmentQueueRepo)
	batchStoreSvc := service.NewBatchStoreService(memoryRepo, projectRepo, nsRepo, ingestionLogRepo, enrichmentQueueRepo, settingsSvc)

	mcpSrv := mcp.NewServer(mcp.Dependencies{
		Backend:       storage.BackendSQLite,
		Store:         storeSvc,
		Recall:        recallSvc,
		Forget:        forgetSvc,
		Update:        updateSvc,
		BatchStore:    batchStoreSvc,
		ProjectRepo:   projectRepo,
		UserRepo:      storage.NewUserRepo(db),
		NamespaceRepo: nsRepo,
		Metrics:       metrics.New(),
	})

	authMw := auth.NewAuthMiddleware(storage.NewAPIKeyRepo(db), storage.NewUserRepo(db), e2eJWTSecret, nil)
	rl := auth.NewRateLimiter(10000, 20000, 0, 0)
	t.Cleanup(rl.Stop)

	router := NewRouter(RouterConfig{
		AuthMiddleware: authMw,
		RateLimiter:    rl,
		Metrics:        metrics.New(),
	}, Handlers{
		MCP: mcpSrv.Handler(),
		Health: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	})
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	jwt, err := auth.GenerateJWT(user.ID, user.OrgID, user.Role, e2eJWTSecret, time.Hour)
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}

	// --- Drive one memory in through the running server -------------------
	sessionID := rbacMCPInitialize(t, ts.URL, jwt)
	rpc := rbacMCPStore(t, ts.URL, jwt, sessionID, project.Slug, "the sky is blue on a clear day")
	if rpc.Error != nil {
		t.Fatalf("MCP store failed: %s", rpc.Error.Message)
	}

	// Find the persisted memory in the project namespace.
	mems, err := memoryRepo.ListByNamespace(ctx, projectNSID, 10, 0)
	if err != nil {
		t.Fatalf("list memories: %v", err)
	}
	if len(mems) != 1 {
		t.Fatalf("expected exactly 1 stored memory, got %d", len(mems))
	}
	memID := mems[0].ID

	// --- Wait for the worker to embed and persist the vector --------------
	var vec []float32
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		got, gerr := vectorStore.GetByIDs(ctx, storage.VectorKindMemory, []uuid.UUID{memID}, embedDim)
		if gerr == nil {
			if v, ok := got[memID]; ok && len(v) > 0 {
				vec = v
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if vec == nil {
		// Surface any failed job to make the failure diagnosable.
		if jobs, lerr := enrichmentQueueRepo.ListRecent(ctx, 5); lerr == nil {
			for _, j := range jobs {
				t.Logf("enrichment job %s: status=%s attempts=%d", j.ID, j.Status, j.Attempts)
			}
		}
		t.Fatalf("no persisted vector for memory %s within deadline; the worker->embed->store chain did not complete", memID)
	}

	// --- Assert the persisted vector row ----------------------------------
	if len(vec) != embedDim {
		t.Fatalf("persisted vector has dimension %d, want %d", len(vec), embedDim)
	}
	nonZero := false
	for _, f := range vec {
		if f != 0 {
			nonZero = true
			break
		}
	}
	if !nonZero {
		t.Fatal("persisted vector is all zeros (null embedding)")
	}

	// Ownership: the embedded memory belongs to the project namespace, and its
	// embedding_dim was recorded on the memory row.
	mem, err := memoryRepo.GetByID(ctx, memID, projectNSID)
	if err != nil {
		t.Fatalf("re-read memory: %v", err)
	}
	if mem.NamespaceID != projectNSID {
		t.Fatalf("memory namespace = %s, want project namespace %s", mem.NamespaceID, projectNSID)
	}
	if mem.EmbeddingDim == nil || *mem.EmbeddingDim != embedDim {
		t.Fatalf("memory embedding_dim = %v, want %d", mem.EmbeddingDim, embedDim)
	}

	// Run correlation: the worker stamps model.EnrichmentRunKey(job.ID, attempts)
	// into token_usage.request_id for every phase of the run, which the
	// enrichment monitor joins on instead of the timestamp window. Assert the
	// end-to-end wiring by locating the job for this memory and confirming at
	// least one of its phase token_usage rows carries that exact key.
	var job *model.EnrichmentJob
	if jobs, lerr := enrichmentQueueRepo.ListRecent(ctx, 10); lerr == nil {
		for i := range jobs {
			if jobs[i].MemoryID == memID {
				job = &jobs[i]
				break
			}
		}
	}
	if job == nil {
		t.Fatal("no enrichment job found for the embedded memory")
	}
	wantKey := model.EnrichmentRunKey(job.ID, job.Attempts)
	ops := make([]string, 0)
	for _, op := range provider.EnrichmentPhaseOperations() {
		ops = append(ops, string(op))
	}
	usageRows, uerr := tokenUsageRepo.ListByMemoryIDs(ctx, []uuid.UUID{memID}, ops)
	if uerr != nil {
		t.Fatalf("list token usage by memory: %v", uerr)
	}
	foundKey := false
	for _, r := range usageRows {
		if r.RequestID != nil && *r.RequestID == wantKey {
			foundKey = true
			break
		}
	}
	if !foundKey {
		got := make([]string, 0, len(usageRows))
		for _, r := range usageRows {
			rid := "<nil>"
			if r.RequestID != nil {
				rid = *r.RequestID
			}
			got = append(got, r.Operation+"="+rid)
		}
		t.Fatalf("no enrichment token_usage row carried run key %q; got %v", wantKey, got)
	}
}

// writeJSON encodes v as a JSON response body, failing the test on error.
func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Errorf("write JSON response: %v", err)
	}
}
