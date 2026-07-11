package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestE2E_EnrichmentFacetWritePath drives a genuinely two-topic memory through
// the full running server and asserts that the enrichment worker wrote topic
// facets (facet_id > 0 rows) into the real vector store.
//
// It closes the coverage gap recorded in the backlog audit: the production facet
// seam runEmbedBatch -> writeMemoryFacets -> extractAndWriteFacets -> UpsertFacets
// (worker.go) had no end-to-end test. facet_live_e2e_test.go embeds with a real
// model but calls ExtractFacets/UpsertFacets directly, bypassing the worker, and
// is NRAM_LIVE_E2E-gated on pgvector/Qdrant so it never runs in CI;
// facet_worker_test.go drives writeMemoryFacets with a fake embedder and a
// recording store, not a real backend; TestE2E_EnrichmentEmbedAndStore stores a
// single-topic memory with a constant embedder, so its facet path is inert.
//
// This test exercises the whole chain in-process with zero external dependencies,
// the same harness as TestE2E_EnrichmentEmbedAndStore, with two differences that
// make faceting actually fire:
//
//   - the fake /v1/embeddings host returns one of two orthogonal unit vectors per
//     input, keyed on a marker token that appears only in the topic-B sentences,
//     so ExtractFacets clusters the memory into exactly two topics; and
//   - the stored memory has two disjoint topics (multiple sentences each).
//
// The facet path needs only a topic-distinguishing embedder, not real fact/entity
// extraction (extractAndWriteFacets clusters the memory's own sentences), so
// /v1/chat/completions still returns an empty array.
//
// The assertion reads the memory's stamped facet_count back (>= 2 proves the
// worker produced topic facets, since a single-topic memory stamps 1) and counts
// the persisted facet_id > 0 rows in the vector store to prove real facet rows
// landed rather than just a state stamp.
func TestE2E_EnrichmentFacetWritePath(t *testing.T) {
	ctx := context.Background()

	// --- Fake OpenAI-compatible provider host -----------------------------
	// marker appears only in the topic-B sentences; the embedding host keys the
	// returned axis on it so topic-A and topic-B sentences embed orthogonally.
	const embedDim = 1024
	const servedModel = "fake-embed-model"
	const marker = "photosynth"
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/models":
			writeJSON(t, w, map[string]any{
				"object": "list",
				"data":   []map[string]any{{"id": servedModel}},
			})
		case "POST /v1/chat/completions":
			// Fact and entity extraction both expect a JSON array; an empty array
			// makes both phases succeed with nothing extracted. The facet path does
			// not depend on extraction, so this is sufficient.
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
			data := make([]map[string]any, len(req.Input))
			for i, in := range req.Input {
				// Two orthogonal unit vectors: axis 1 for topic-B (marker) inputs,
				// axis 0 for everything else. Cosine within a topic is 1, between
				// topics 0, so AnchorClusters (threshold 0.65) yields two clusters.
				axis := 0
				if strings.Contains(strings.ToLower(in), marker) {
					axis = 1
				}
				vec := make([]float32, embedDim)
				vec[axis] = 1.0
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

	projectNSID := uuid.New()
	projectNS := &model.Namespace{
		ID:       projectNSID,
		Name:     "facet-e2e",
		Slug:     "facet-e2e",
		Kind:     "project",
		ParentID: &user.NamespaceID,
		Path:     "/users/facet-e2e/" + projectNSID.String(),
		Depth:    3,
	}
	if err := nsRepo.Create(ctx, projectNS); err != nil {
		t.Fatalf("create project namespace: %v", err)
	}
	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      projectNSID,
		OwnerNamespaceID: user.NamespaceID,
		Name:             "Facet E2E",
		Slug:             "facet-e2e",
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
	// enrichment.multi_vector.enabled defaults to true, so the worker's facet
	// path is active without extra wiring. The read-side facet gate that boot
	// installs (SetFacetGate) affects recall, not writes, so it is not needed to
	// assert facet rows land.
	if !settingsSvc.ResolveBoolWithDefault(ctx, service.SettingMultiVectorEnabled, "global") {
		t.Fatal("multi-vector faceting is not enabled by default; the test presumes it is")
	}
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

	// --- Drive one two-topic memory in through the running server ---------
	// Topic A (castles) carries no marker; topic B (photosynthesis) carries the
	// marker in every sentence, so the two topics embed onto orthogonal axes.
	content := "Medieval castles relied on thick stone curtain walls for defense. " +
		"A moat and a raised drawbridge guarded the castle gatehouse against attackers. " +
		"Photosynthesis converts sunlight into chemical energy inside plant chloroplasts. " +
		"During photosynthesis, chlorophyll absorbs light and splits water to release oxygen."

	sessionID := rbacMCPInitialize(t, ts.URL, jwt)
	rpc := rbacMCPStore(t, ts.URL, jwt, sessionID, project.Slug, content)
	if rpc.Error != nil {
		t.Fatalf("MCP store failed: %s", rpc.Error.Message)
	}

	mems, err := memoryRepo.ListByNamespace(ctx, projectNSID, 10, 0)
	if err != nil {
		t.Fatalf("list memories: %v", err)
	}
	if len(mems) != 1 {
		t.Fatalf("expected exactly 1 stored memory, got %d", len(mems))
	}
	memID := mems[0].ID

	// --- Wait for the worker to embed, facet, and stamp facet state -------
	var mem *model.Memory
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		got, gerr := memoryRepo.GetByID(ctx, memID, projectNSID)
		if gerr == nil && got.FacetedAt != nil && got.FacetCount != nil {
			mem = got
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if mem == nil {
		if jobs, lerr := enrichmentQueueRepo.ListRecent(ctx, 5); lerr == nil {
			for _, j := range jobs {
				t.Logf("enrichment job %s: status=%s attempts=%d", j.ID, j.Status, j.Attempts)
			}
		}
		t.Fatalf("memory %s was never stamped with facet state within deadline; the worker facet path did not complete", memID)
	}

	// --- Assert topic facets were produced and persisted ------------------
	// facet_count is stamped as len(facets) = facet 0 (pooled) + one per topic,
	// so a two-topic memory stamps >= 3.
	if *mem.FacetCount < 3 {
		t.Fatalf("facet_count = %d, want >= 3 (facet 0 + two topic facets); the two topics did not cluster through the worker", *mem.FacetCount)
	}

	// The persisted facet rows: facet 0 plus the topic facets. Count the topic
	// facets (facet_id > 0) and confirm they equal facet_count - 1, proving real
	// facet rows landed in the vector store rather than only a state stamp.
	var topicFacetRows int
	if err := db.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM memory_vectors WHERE memory_id = ? AND facet_id > 0 AND dimension = ?",
		memID.String(), embedDim,
	).Scan(&topicFacetRows); err != nil {
		t.Fatalf("count topic facet rows: %v", err)
	}
	if want := *mem.FacetCount - 1; topicFacetRows != want {
		t.Fatalf("persisted topic facet rows = %d, want %d (facet_count %d minus facet 0)", topicFacetRows, want, *mem.FacetCount)
	}
}
