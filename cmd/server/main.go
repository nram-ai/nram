package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/dreaming"
	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/mcp"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/server"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
	adminstore "github.com/nram-ai/nram/internal/storage/admin"
	"github.com/nram-ai/nram/internal/ui"
)

// runHeadlessBootstrap creates the first administrator from the bootstrap
// admin credentials when the database is empty. Returns true when setup is
// complete after the call (either it already was, or this call just made it
// so), letting the caller seed the cached SetupChecker without re-querying
// the DB. Idempotent — re-running with the same credentials is safe.
func runHeadlessBootstrap(ctx context.Context, store *adminstore.SetupStore, adminCfg config.AdminConfig) bool {
	complete, err := store.IsSetupComplete(ctx)
	if err != nil {
		log.Printf("headless bootstrap: setup-status check failed: %v", err)
		return false
	}
	if complete {
		return true
	}

	switch {
	case adminCfg.Email == "" && adminCfg.Password == "":
		return false
	case adminCfg.Email == "" || adminCfg.Password == "":
		log.Printf("headless bootstrap: skipping — both admin.email and admin.password (or NRAM_ADMIN_EMAIL/NRAM_ADMIN_PASS) must be set")
		return false
	}

	user, _, err := store.CompleteSetup(ctx, adminCfg.Email, adminCfg.Password)
	if err != nil {
		log.Printf("headless bootstrap: failed to create administrator %s: %v", adminCfg.Email, err)
		return false
	}
	log.Printf("headless bootstrap: created administrator %s (id=%s)", user.Email, user.ID)
	return true
}

// configureLogger installs a slog text handler at the level named by
// cfg.LogLevel (info|debug|warn|error). Without this, slog defaults to INFO
// and the cfg.LogLevel field is read but never honoured.
func configureLogger(level string) {
	var l slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug", "trace":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: l,
	})))
}

func main() {
	// Determine config file path from --config flag if provided.
	var configPath string
	for i, arg := range os.Args[1:] {
		if arg == "--config" && i+1 < len(os.Args[1:]) {
			configPath = os.Args[i+2]
			break
		}
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	configureLogger(cfg.LogLevel)

	db, err := storage.Open(cfg.Database)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	log.Printf("database backend: %s", db.Backend())

	// Handle migration CLI commands before starting the server.
	handled, err := migration.RunCLI(os.Args, db.WriteDB(), db.Backend())
	if err != nil {
		log.Fatalf("migration command failed: %v", err)
	}
	if handled {
		return
	}

	// Auto-migrate on startup if configured.
	if cfg.Database.MigrateOnStart {
		m, err := migration.NewMigrator(db.WriteDB(), db.Backend())
		if err != nil {
			log.Fatalf("failed to create migrator: %v", err)
		}
		if err := m.Up(); err != nil {
			log.Fatalf("auto-migration failed: %v", err)
		}
		m.Close()
		log.Println("migrations applied successfully")
	}

	for _, arg := range os.Args[1:] {
		if arg == "--backfill-enrichment" {
			n, err := storage.EnqueueUncoveredMemories(context.Background(), db)
			if err != nil {
				log.Fatalf("enrichment backfill failed: %v", err)
			}
			log.Printf("backfill: enqueued %d enrichment jobs", n)
			return
		}
		if arg == "--reembed-all-memories" {
			n, err := storage.EnqueueAllLiveMemories(context.Background(), db)
			if err != nil {
				log.Fatalf("reembed all memories failed: %v", err)
			}
			log.Printf("reembed: enqueued %d memory re-embed jobs (force, every live memory)", n)
			return
		}
	}

	if os.Getenv("NRAM_ENABLE_ENRICHMENT_BACKFILL") == "1" {
		n, err := storage.EnqueueUncoveredMemories(context.Background(), db)
		if err != nil {
			log.Fatalf("startup enrichment backfill failed: %v", err)
		}
		log.Printf("backfill: enqueued %d enrichment jobs at startup (NRAM_ENABLE_ENRICHMENT_BACKFILL=1)", n)
	}

	// Create repositories.
	memoryRepo := storage.NewMemoryRepo(db)
	projectRepo := storage.NewProjectRepo(db)
	namespaceRepo := storage.NewNamespaceRepo(db)

	// Populate content_hash for legacy rows synchronously so MemoryRepo.Create
	// can rely on the column from the first request.
	{
		ctx := context.Background()
		total := 0
		for {
			n, err := memoryRepo.BackfillContentHashes(ctx, 1000)
			if err != nil {
				log.Fatalf("content_hash backfill failed after %d rows: %v", total, err)
			}
			if n == 0 {
				break
			}
			total += n
		}
		if total > 0 {
			log.Printf("content_hash backfill: populated %d rows", total)
		}
	}

	// Ensure every user has a "global" project. This is idempotent — existing
	// global projects are skipped. Handles upgrades from versions before the
	// global project was introduced.
	{
		tmpUserRepo := storage.NewUserRepo(db)
		users, err := tmpUserRepo.ListAll(context.Background())
		if err == nil {
			for _, u := range users {
				_, _ = projectRepo.AutoCreateUnderUser(context.Background(), namespaceRepo, u.NamespaceID, "global")
			}
		}
	}
	userRepo := storage.NewUserRepo(db)
	orgRepo := storage.NewOrganizationRepo(db)
	apiKeyRepo := storage.NewAPIKeyRepo(db)
	webauthnRepo := storage.NewWebAuthnRepo(db)
	oauthRepo := storage.NewOAuthRepo(db)
	entityRepo := storage.NewEntityRepo(db)
	entityAliasRepo := storage.NewEntityAliasRepo(db)
	relationshipRepo := storage.NewRelationshipRepo(db)
	lineageRepo := storage.NewMemoryLineageRepo(db)
	shareRepo := storage.NewMemoryShareRepo(db)
	webhookRepo := storage.NewWebhookRepo(db)
	ingestionLogRepo := storage.NewIngestionLogRepo(db)
	tokenUsageRepo := storage.NewTokenUsageRepo(db)
	enrichmentQueueRepo := storage.NewEnrichmentQueueRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	settingsSvc := service.NewSettingsService(settingsRepo)

	// Seed registered defaults at boot so SELECT key FROM settings reflects
	// the full schema surface area instead of the subset operators have
	// touched. Idempotent; never overwrites operator-set values.
	if err := seedRegisteredSettings(context.Background(), settingsRepo); err != nil {
		log.Printf("warning: settings seed failed: %v", err)
	}

	// Create provider registry. Provider configuration lives in the DB
	// settings table (provider.{embedding,fact,entity}) and is managed via
	// the admin UI. On a fresh install the slots are empty and the registry
	// reports providers unavailable until an admin completes setup.
	regCfg := adminstore.LoadProviderRegistryConfig(context.Background(), settingsRepo)
	registry, err := provider.NewRegistry(regCfg, tokenUsageRepo, namespaceRepo)
	if err != nil {
		log.Printf("warning: provider registry init failed (providers disabled): %v", err)
		registry = nil
	}

	embedProvider := func() provider.EmbeddingProvider {
		if registry == nil {
			return nil
		}
		return registry.GetEmbedding()
	}

	// Resolve Qdrant connection settings from the runtime registry. Operators
	// configure Qdrant through /v1/admin/settings under the qdrant.* keys; an
	// empty addr means Qdrant is not in use and the vector store falls back
	// to pgvector or HNSW depending on the database backend.
	bootCtx := context.Background()
	qdrantCfg := storage.QdrantConfig{
		Addr:             service.ResolveOrDefault(bootCtx, settingsSvc, service.SettingQdrantAddr, "global"),
		APIKey:           service.ResolveOrDefault(bootCtx, settingsSvc, service.SettingQdrantAPIKey, "global"),
		UseTLS:           settingsSvc.ResolveBool(bootCtx, service.SettingQdrantUseTLS, "global"),
		PoolSize:         uint(settingsSvc.ResolveIntWithDefault(bootCtx, service.SettingQdrantPoolSize, "global")),
		KeepAliveTime:    settingsSvc.ResolveIntWithDefault(bootCtx, service.SettingQdrantKeepAliveTime, "global"),
		KeepAliveTimeout: uint(settingsSvc.ResolveIntWithDefault(bootCtx, service.SettingQdrantKeepAliveTimeout, "global")),
	}

	// Create vector store.
	// Priority: Qdrant (if configured) > PgVector (if Postgres) > HNSWStore (if SQLite).
	var vectorStore storage.VectorStore
	var hnswStore *storage.HNSWStore
	if qdrantCfg.Addr != "" {
		vectorStore, err = storage.NewQdrantStore(qdrantCfg)
		if err != nil {
			log.Printf("warning: qdrant connection failed (vector search disabled): %v", err)
		}
	}
	if vectorStore == nil && db.Backend() == storage.BackendPostgres && cfg.Database.URL != "" {
		pgvStore, pgvErr := storage.NewPgVectorStore(cfg.Database.URL)
		if pgvErr != nil {
			log.Printf("warning: pgvector connection failed (vector search disabled): %v", pgvErr)
		} else {
			vectorStore = pgvStore
			log.Println("pgvector store initialized")
		}
	}
	if vectorStore == nil && db.Backend() == storage.BackendSQLite {
		hnswCfg := storage.HNSWConfig{
			M:                settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingHNSWM, "global"),
			EfConstruction:   settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingHNSWEfConstruction, "global"),
			EfSearch:         settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingHNSWEfSearch, "global"),
			MaxLoadedIndexes: settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingHNSWMaxLoadedIndexes, "global"),
		}
		hnswStore = storage.NewHNSWStore(db.DB(), db.WriteDB(), hnswCfg)
		vectorStore = hnswStore
		defer hnswStore.Close()
		log.Printf("hnsw vector store initialized (SQLite backend; M=%d ef_construction=%d ef_search=%d max_loaded=%d)",
			hnswCfg.M, hnswCfg.EfConstruction, hnswCfg.EfSearch, hnswCfg.MaxLoadedIndexes)
	}

	// Create event bus. Buffer and replay capacity are read once from
	// settings; runtime changes require server restart.
	eventBusBuf := settingsSvc.ResolveIntWithDefault(context.Background(),
		service.SettingEventsSubscriberBufferSize, "global")
	eventBusReplay := settingsSvc.ResolveIntWithDefault(context.Background(),
		service.SettingEventsReplayCapacity, "global")
	eventBus := events.NewEventBus(db.Backend(), nil, eventBusBuf, eventBusReplay)
	defer eventBus.Close()

	// Create webhook deliverer.
	webhookDeliverer := events.NewWebhookDeliverer(eventBus, webhookRepo)
	delivererCtx, delivererCancel := context.WithCancel(context.Background())
	defer delivererCancel()
	go func() {
		if err := webhookDeliverer.Start(delivererCtx); err != nil {
			log.Printf("webhook deliverer stopped: %v", err)
		}
	}()

	// Create services.
	storeSvc := service.NewStoreService(
		memoryRepo, projectRepo, namespaceRepo,
		ingestionLogRepo, enrichmentQueueRepo,
		settingsSvc,
	)
	recallSvc := service.NewRecallService(
		memoryRepo, projectRepo, namespaceRepo,
		vectorStore, entityRepo,
		relationshipRepo, shareRepo, embedProvider,
	)
	updateSvc := service.NewUpdateService(
		memoryRepo, projectRepo,
		vectorStore, embedProvider, enrichmentQueueRepo,
	)
	forgetSvc := service.NewForgetService(
		memoryRepo, projectRepo, vectorStore,
		lineageRepo,
	)
	batchGetSvc := service.NewBatchGetService(memoryRepo, projectRepo)
	batchStoreSvc := service.NewBatchStoreService(
		memoryRepo, projectRepo, namespaceRepo,
		ingestionLogRepo, enrichmentQueueRepo,
		settingsSvc,
	)
	var hnswDeleter service.HNSWSnapshotDeleter
	if hnswStore != nil {
		hnswDeleter = hnswStore
	}
	projectDeleteSvc := service.NewProjectDeleteService(
		projectRepo, projectRepo, memoryRepo, memoryRepo,
		vectorStore, entityRepo, relationshipRepo,
		enrichmentQueueRepo, tokenUsageRepo,
		ingestionLogRepo, shareRepo, hnswDeleter, namespaceRepo, eventBus,
	)
	enrichSvc := service.NewEnrichService(memoryRepo, projectRepo, enrichmentQueueRepo, lineageRepo)
	exportSvc := service.NewExportService(
		memoryRepo, entityRepo, relationshipRepo, lineageRepo, projectRepo,
		settingsSvc,
	)
	importSvc := service.NewImportService(
		memoryRepo, projectRepo, namespaceRepo, ingestionLogRepo,
		settingsSvc,
	)

	// Reconsolidation hook on recall. Mode defaults to shadow (observable-only
	// via events.MemoryReinforced) so the first deployment emits without
	// mutating the DB; flip to persist via the reconsolidation.mode setting
	// once the shadow run looks right.
	recallSvc.SetReinforcement(&service.ReinforcementDeps{
		Writer:    memoryRepo,
		RelWriter: relationshipRepo,
		Settings:  settingsSvc,
		Bus:       eventBus,
		Scope:     "global",
	})

	// Hybrid recall fusion. The lexical channel is the same MemoryRepo
	// (its SearchByText hits FTS5 on SQLite or the content_tsv generated
	// column on Postgres). FusionConfig is loaded from /v1/admin/settings,
	// off by default — flipping recall.fusion.enabled is the deployment
	// switch after migrations have been applied.
	recallSvc.SetLexical(memoryRepo)
	recallSvc.SetFusion(loadFusionConfig(context.Background(), settingsSvc))
	recallSvc.SetWeights(loadRankingWeights(context.Background(), settingsSvc))
	recallSvc.SetSettings(settingsSvc)

	// Create lifecycle service for TTL expiry and purge sweeps. Sweep
	// interval, batch size, and orphan-grace cutoff are all read live from
	// the settings registry (lifecycle.* keys) so operators can tune them
	// from the admin UI without restarting.
	graphPruner := service.NewGraphPruner(entityRepo, relationshipRepo)
	lifecycleSvc := service.NewLifecycleService(memoryRepo, vectorStore, graphPruner, service.LifecycleConfig{}, settingsSvc)
	lifecycleSvc.Start()
	defer lifecycleSvc.Stop()

	// Read live so a hot provider reload reopens or closes the gate
	// without a restart.
	enrichmentAvailable := func() bool {
		return registry != nil && registry.EnrichmentAvailable()
	}

	// Create MCP server.
	mcpServer := mcp.NewServer(mcp.Dependencies{
		Backend:        db.Backend(),
		Store:          storeSvc,
		Recall:         recallSvc,
		Forget:         forgetSvc,
		Update:         updateSvc,
		BatchGet:       batchGetSvc,
		BatchStore:     batchStoreSvc,
		Enrich:         enrichSvc,
		Export:         exportSvc,
		ProjectDelete:  projectDeleteSvc,
		ProjectUpdater: projectRepo,
		ProjectRepo:    projectRepo,
		UserRepo:       userRepo,
		NamespaceRepo:  namespaceRepo,
		MemoryLister:   memoryRepo,
		EntityReader:   entityRepo,
		Traverser:      relationshipRepo,
		EventBus:       eventBus,
		ProviderStatus: func() (bool, bool) {
			if registry == nil {
				return false, false
			}
			hasEmbed := registry.GetEmbedding() != nil
			hasEnrich := registry.GetFact() != nil && registry.GetEntity() != nil
			return hasEmbed, hasEnrich
		},
		EnrichmentAvailable: enrichmentAvailable,
	})

	// Create metrics.
	metrics := api.NewMetrics()

	// Build start time for health handler.
	startTime := time.Now()

	// Create setup checker (cached atomic bool, queries DB once).
	setupChecker := api.NewSetupChecker(db)

	// Create admin store adapters.
	setupStore := adminstore.NewSetupStore(userRepo, namespaceRepo, orgRepo, apiKeyRepo, projectRepo, db)

	// Headless administrator bootstrap. When admin.email and admin.password
	// are both supplied via config.yaml or NRAM_ADMIN_EMAIL/NRAM_ADMIN_PASS,
	// the first administrator is created automatically — bypassing the setup
	// wizard. After setup is complete the call is a no-op so re-running with
	// the same env vars is safe across restarts.
	if runHeadlessBootstrap(context.Background(), setupStore, cfg.Admin) {
		setupChecker.MarkComplete()
	}

	orgAdminStore := adminstore.NewOrgAdminStore(orgRepo, namespaceRepo)
	userAdminStore := adminstore.NewUserAdminStore(userRepo, apiKeyRepo, namespaceRepo, orgRepo, projectRepo)
	projectAdminStore := adminstore.NewProjectAdminStore(db, projectRepo, namespaceRepo)
	webhookAdminStore := adminstore.NewWebhookAdminStore(webhookRepo)
	settingsAdminStore := adminstore.NewSettingsAdminStore(settingsRepo)
	dashboardStore := adminstore.NewDashboardStore(db, enrichmentQueueRepo)
	analyticsStore := adminstore.NewAnalyticsStore(db)
	usageStore := adminstore.NewUsageStore(db)
	aggregatesStore := adminstore.NewAggregatesStore(db)
	auditStore := adminstore.NewAuditStore(db)
	databaseAdminStore := adminstore.NewDatabaseAdminStore(db)
	namespaceAdminStore := adminstore.NewNamespaceAdminStore(db)
	providerAdminStore := adminstore.NewProviderAdminStore(adminstore.ProviderAdminDeps{
		Registry:     registry,
		SettingsRepo: settingsRepo,
		Settings:     settingsSvc,
		MemoryRepo:   memoryRepo,
		EntityRepo:   entityRepo,
		VectorStore:  vectorStore,
		DB:           db,
	})
	oauthAdminStore := adminstore.NewOAuthAdminStore(oauthRepo)
	enrichmentAdminStore := adminstore.NewEnrichmentAdminStore(enrichmentQueueRepo, settingsRepo, settingsSvc, db)

	// Provider accessors for enrichment test prompt.
	factProvider := func() provider.LLMProvider {
		if registry == nil {
			return nil
		}
		return registry.GetFact()
	}
	entityProvider := func() provider.LLMProvider {
		if registry == nil {
			return nil
		}
		return registry.GetEntity()
	}

	// Ingestion-decision deduplicator. Wires the existing dedup vector
	// search into the enrichment worker so context-aware deduplication runs
	// at write time. Disabled by default in settings; flipping the toggle
	// activates the phase without further plumbing.
	ingestionDedup := enrichment.NewDeduplicator(vectorStore, embedProvider, memoryRepo, enrichment.DefaultDeduplicationConfig)

	// Cascade resolver merges system + user + project layers of the
	// enrichment_enabled and dedup_threshold overrides. The worker pool
	// honors enrichment_enabled at both the master toggle (system-only,
	// uuid.Nil namespace) and per-job (memory.namespace_id) layers.
	cascadeResolver := service.NewCascadeResolver(settingsSvc, projectRepo, userRepo)

	// Start enrichment worker pool — needs providers for LLM extraction.
	workerPool := enrichment.NewWorkerPool(
		enrichment.WorkerConfig{Backend: db.Backend()},
		memoryRepo, memoryRepo, memoryRepo, memoryRepo, enrichmentQueueRepo,
		entityRepo, relationshipRepo, lineageRepo, vectorStore,
		factProvider, entityProvider, embedProvider,
		factProvider, ingestionDedup, settingsSvc, cascadeResolver,
		eventBus,
	)
	workerPool.Start()
	defer workerPool.Stop()
	log.Println("enrichment worker pool started")

	// Warn (once) if any provider-load knob is raised above its safe default.
	service.CheckProviderLoadDefaults(context.Background(), settingsSvc)

	// Sweeper recovers rows wedged in 'processing' by dead workers. Deferred
	// Stop runs before workerPool.Stop (LIFO) so a final sweep on shutdown
	// does not race a worker normally finishing its batch.
	enrichmentStuckSweeper := enrichment.NewStuckJobSweeper(
		enrichmentQueueRepo, settingsSvc, eventBus,
	)
	enrichmentStuckSweeper.Start()
	defer enrichmentStuckSweeper.Stop()
	log.Println("enrichment stuck-job sweeper started")

	// Create dreaming system.
	dreamCycleRepo := storage.NewDreamCycleRepo(db)
	dreamLogRepo := storage.NewDreamLogRepo(db)
	dreamDirtyRepo := storage.NewDreamDirtyRepo(db)

	consolidationPhase := dreaming.NewConsolidationPhase(memoryRepo, memoryRepo, lineageRepo, factProvider, embedProvider, settingsSvc)
	contradictionPhase := dreaming.NewContradictionPhase(memoryRepo, memoryRepo, lineageRepo, factProvider, embedProvider, settingsSvc)
	// Wire the active vector store into dream-side state transitions so that
	// demotion and supersession purge vectors alongside the row-level update,
	// and so the contradiction phase reads stored vectors instead of
	// re-embedding the namespace every cycle.
	if vectorStore != nil {
		consolidationPhase.AttachVectorPurger(vectorStore)
		contradictionPhase.AttachVectorStore(vectorStore)
		contradictionPhase.AttachVectorPurger(vectorStore)
		memoryRepo.AttachVectorStore(vectorStore)
	}

	heartbeatInterval := settingsSvc.ResolveDurationSecondsWithDefault(
		context.Background(), service.SettingDreamHeartbeatInterval, "global")

	dreamRunner := dreaming.NewRunner(
		dreamCycleRepo, dreamLogRepo, workerPool, heartbeatInterval, eventBus, settingsSvc,
		dreaming.NewEntityDedupPhase(entityRepo, entityRepo, entityAliasRepo, relationshipRepo, relationshipRepo, vectorStore, settingsSvc),
		// Embedding backfill repairs rows whose embedding_dim is set but
		// whose memory_vectors_<dim> row is missing (no_vector divergence).
		// Runs before paraphrase dedup so the downstream phase sees the
		// repaired vector state in the same cycle.
		dreaming.NewEmbeddingBackfillPhase(memoryRepo, memoryRepo, vectorStore, embedProvider, settingsSvc),
		// Paraphrase dedup runs before contradiction so the LLM-judge pair
		// walk operates on a deduped memory set.
		dreaming.NewParaphraseDedupPhase(memoryRepo, memoryRepo, vectorStore, vectorStore, embedProvider, settingsSvc),
		dreaming.NewTransitivePhase(entityRepo, relationshipRepo, relationshipRepo, settingsSvc),
		contradictionPhase,
		consolidationPhase,
		dreaming.NewPruningPhase(memoryRepo, memoryRepo, relationshipRepo, settingsSvc),
		dreaming.NewWeightAdjustmentPhase(entityRepo, entityRepo, relationshipRepo, relationshipRepo, memoryRepo, settingsSvc),
	)

	dreamRollback := dreaming.NewRollbackService(
		dreamLogRepo, dreamCycleRepo, dreamDirtyRepo,
		memoryRepo, memoryRepo, relationshipRepo, entityRepo, entityRepo,
	)

	dreamRetention := dreaming.NewRetentionSweeper(dreamLogRepo, dreamCycleRepo, memoryRepo, settingsSvc)

	// Start dirty tracker (event subscriber).
	dirtyTracker := dreaming.NewDirtyTracker(eventBus, dreamDirtyRepo)
	trackerCtx, trackerCancel := context.WithCancel(context.Background())
	defer trackerCancel()
	if err := dirtyTracker.Start(trackerCtx); err != nil {
		log.Printf("dream dirty tracker failed to start: %v", err)
	}
	defer dirtyTracker.Stop()

	// Start dream scheduler. The scheduler stays running even when the
	// enrichment gate is closed; its poll skips when EnrichmentAvailable
	// returns false, so a live provider reload reopens dreaming without
	// restart.
	dreamScheduler := dreaming.NewScheduler(
		dreaming.SchedulerConfig{EnrichmentAvailable: enrichmentAvailable},
		settingsSvc, cascadeResolver, dreamDirtyRepo, dreamCycleRepo,
		projectRepo, workerPool, dreamRunner, eventBus, dreamRetention,
	)
	dreamScheduler.Start()
	defer dreamScheduler.Stop()
	log.Println("dream scheduler started")

	// Start the stuck-cycle sweeper in its own goroutine. Lifecycle is
	// independent of the scheduler so a long-running cycle on this instance
	// (which blocks the scheduler's main loop) can't also block the sweeper
	// that's supposed to detect and recover from it.
	dreamStuckSweeper := dreaming.NewStuckCycleSweeper(
		dreamCycleRepo, dreamScheduler, settingsSvc, eventBus,
	)
	dreamStuckSweeper.Start()
	defer dreamStuckSweeper.Stop()
	log.Println("dream stuck-cycle sweeper started")

	dreamAdminStore := adminstore.NewDreamAdminStore(
		dreamCycleRepo, dreamLogRepo, dreamDirtyRepo, settingsRepo,
		settingsSvc, dreamScheduler, projectRepo, cascadeResolver,
	)

	// Create auth config for login/lookup handlers.
	// JWT secret is loaded later, but we need it here — load it early.
	jwtSecret, err := storage.LoadOrCreateJWTSecret(context.Background(), db)
	if err != nil {
		log.Fatalf("failed to load jwt secret: %v", err)
	}

	// Create OAuth server. Base URL for metadata, JWT audience, etc. is derived
	// from the request Host header automatically — no configuration needed.
	oauthServer := auth.NewOAuthServer(oauthRepo, userRepo, jwtSecret)

	authCfg := api.AuthConfig{
		UserRepo:    userRepo,
		IdPRepo:     oauthRepo,
		PasskeyRepo: webauthnRepo,
		JWTSecret:   jwtSecret,
	}

	// Create WebAuthn handler for passkey registration and login.
	webauthnHandler := auth.NewWebAuthnHandler(auth.WebAuthnHandlerConfig{
		CredRepo:  webauthnRepo,
		UserRepo:  userRepo,
		JWTSecret: jwtSecret,
	})
	defer webauthnHandler.Close()

	// Create IdP SSO handler for external identity provider flows.
	idpHandler := auth.NewIdPHandler(auth.IdPHandlerConfig{
		IdPRepo:    oauthRepo,
		UserRepo:   userRepo,
		UserCreate: userAdminStore,
		JWTSecret:  jwtSecret,
	})

	// Assemble handlers.
	handlers := server.Handlers{
		// Health
		Health: api.NewHealthHandler(api.HealthConfig{
			DB:        db,
			Providers: registry,
			Queue:     enrichmentQueueRepo,
			Version:   "0.1.0",
			StartTime: startTime,
		}),

		// Project-scoped memory handlers
		Store:      api.NewStoreHandler(storeSvc, eventBus),
		List:       api.NewListHandler(memoryRepo, projectRepo, lineageRepo),
		ListIDs:    api.NewListIDsHandler(memoryRepo, projectRepo),
		Detail:     api.NewDetailHandler(memoryRepo, projectRepo, lineageRepo),
		Update:     api.NewUpdateHandler(updateSvc, eventBus),
		Delete:     api.NewDeleteHandler(forgetSvc, eventBus),
		BatchStore: api.NewBatchStoreHandler(batchStoreSvc, eventBus),
		BatchGet:   api.NewBatchGetHandler(batchGetSvc),
		Recall:     api.NewRecallHandler(recallSvc),
		BulkForget: api.NewBulkForgetHandler(forgetSvc, eventBus),
		Enrich:     api.NewEnrichHandler(enrichSvc, eventBus),
		Export:     api.NewExportHandler(exportSvc),
		Import:     api.NewImportHandler(importSvc),

		// User-scoped handlers
		MeRecall:            api.NewMeRecallHandler(recallSvc, userRepo),
		MeProjects:          api.NewMeProjectsHandler(projectRepo, userRepo, namespaceRepo),
		MeProjectItem:       api.NewMeProjectItemHandler(projectRepo, userRepo),
		MeProjectDelete:     api.NewMeProjectDeleteHandler(projectDeleteSvc, projectRepo, userRepo),
		MeAPIKeys:           api.NewMeAPIKeysHandler(apiKeyRepo, auditStore),
		MeAPIKeyRevoke:      api.NewMeAPIKeyRevokeHandler(apiKeyRepo, auditStore),
		MeOAuthClients:      api.NewMeOAuthClientsHandler(oauthRepo),
		MeOAuthClientRevoke: api.NewMeOAuthClientRevokeHandler(oauthRepo, auditStore),
		MeChangePassword:    api.NewMeChangePasswordHandler(userRepo, auditStore),
		MeProfile:           api.NewMeProfileHandler(userRepo),
		MeProfilePatch:      api.NewMeProfilePatchHandler(userRepo),

		// Self-tier system-pipeline observability — read-only views of the
		// caller's own dream cycles + enrichment queue items. Write
		// operations (enable/abandon/rollback for dreaming, retry/pause
		// for enrichment) remain admin-only at /v1/admin/*.
		MeDreaming: api.NewSelfDreamingHandler(api.MeDreamingConfig{
			Store:      dreamAdminStore,
			Projects:   projectRepo,
			Namespaces: namespaceRepo,
			Users:      userRepo,
		}),
		MeEnrichment: api.NewSelfEnrichmentHandler(api.MeEnrichmentConfig{
			Store: enrichmentAdminStore,
			Users: userRepo,
		}),
		MeCapabilities: api.NewMeCapabilitiesHandler(api.MeCapabilitiesConfig{
			EnrichmentAvailable: enrichmentAvailable,
			Settings:            settingsSvc,
		}),

		// Passkey management handlers. Register-finish gets an
		// audit-on-success wrapper because the upstream webauthn handler
		// returns http.Handler and we don't want to fork its package.
		MePasskeysList:         api.NewMePasskeysListHandler(webauthnRepo),
		MePasskeyRegisterBegin: webauthnHandler.RegisterBeginHandler(),
		MePasskeyRegisterFinish: api.AuditOnSuccess(
			auditStore,
			api.AuditActionPasskeyRegister,
			"passkey",
			webauthnHandler.RegisterFinishHandler(),
		).ServeHTTP,
		MePasskeyDelete: api.NewMePasskeyDeleteHandler(webauthnRepo, auditStore),

		// Org-scoped handlers
		OrgUsers: api.NewOrgUsersHandler(api.OrgUserConfig{Store: userAdminStore}),
		OrgIdP:   api.NewOrgIdPHandler(oauthRepo),

		// SSE events
		Events: api.NewEventsHandler(eventBus,
			settingsSvc.ResolveDurationSecondsWithDefault(context.Background(),
				service.SettingEventsSSEKeepaliveSeconds, "global")),

		// MCP server
		MCP: mcpServer.Handler(),

		// Embedded admin UI
		UI: ui.Handler(),

		// Auth handlers
		AuthLogin:         api.NewLoginHandler(authCfg),
		AuthLookup:        api.NewLookupHandler(authCfg),
		AuthPasskeyBegin:  webauthnHandler.LoginBeginHandler(),
		AuthPasskeyFinish: webauthnHandler.LoginFinishHandler(),

		// OAuth handlers
		OAuthAuthorize:         oauthServer.AuthorizeHandler(),
		OAuthToken:             oauthServer.TokenHandler(),
		OAuthRegister:          oauthServer.RegisterClientHandler(),
		OAuthUserInfo:          oauthServer.UserInfoHandler(),
		OAuthMetadata:          oauthServer.MetadataHandler(),
		OAuthProtectedResource: oauthServer.ProtectedResourceHandler(),

		// IdP SSO handlers
		IdPLogin: idpHandler.LoginHandler(),
		IdPCallback: api.AuditOnSuccess(
			auditStore,
			api.AuditActionIdPLogin,
			"user",
			idpHandler.CallbackHandler(),
		).ServeHTTP,

		// Admin handlers
		AdminSetupStatus: api.NewAdminSetupStatusHandler(api.SetupConfig{Store: setupStore}),
		AdminSetup: api.NewAdminSetupHandler(api.SetupConfig{
			Store:      setupStore,
			JWTSecret:  jwtSecret,
			OnComplete: setupChecker.MarkComplete,
			Audit:      auditStore,
		}),
		AdminDashboard: api.NewAdminDashboardHandler(api.DashboardConfig{Store: dashboardStore}),
		AdminActivity:  api.NewAdminActivityHandler(api.DashboardConfig{Store: dashboardStore}),
		AdminOrgs:  api.NewAdminOrgsHandler(api.OrgAdminConfig{Store: orgAdminStore, Audit: auditStore}),
		AdminUsers: api.NewAdminUsersHandler(api.UserAdminConfig{Store: userAdminStore, Audit: auditStore}),
		AdminProjects:  api.NewAdminProjectsHandler(api.ProjectAdminConfig{Store: projectAdminStore}),
		AdminProviders: api.NewAdminProvidersHandler(api.ProviderAdminConfig{Store: providerAdminStore}),
		AdminSettings:  api.NewAdminSettingsHandler(api.SettingsAdminConfig{Store: settingsAdminStore}),
		AdminEnrichment: api.NewAdminEnrichmentHandler(api.EnrichmentAdminConfig{
			Store:          enrichmentAdminStore,
			FactProvider:   factProvider,
			EntityProvider: entityProvider,
			FactPromptDefault: func(ctx context.Context) string {
				return service.ResolveOrDefault(ctx, settingsSvc, service.SettingFactPrompt, "global")
			},
			EntityPromptDefault: func(ctx context.Context) string {
				return service.ResolveOrDefault(ctx, settingsSvc, service.SettingEntityPrompt, "global")
			},
		}),
		AdminOAuth:      api.NewAdminOAuthHandler(api.OAuthAdminConfig{Store: oauthAdminStore}),
		AdminWebhooks:   api.NewAdminWebhooksHandler(api.WebhookAdminConfig{Store: webhookAdminStore}),
		AdminAnalytics:  api.NewAdminAnalyticsHandler(api.AnalyticsConfig{Store: analyticsStore}),
		AdminUsage:      api.NewAdminUsageHandler(api.UsageConfig{Store: usageStore}),
		AdminNamespaces: api.NewAdminNamespacesHandler(api.NamespaceAdminConfig{Store: namespaceAdminStore}),
		AdminDatabase:   api.NewAdminDatabaseHandler(api.DatabaseAdminConfig{Store: databaseAdminStore}),
		AdminGraph: api.NewAdminGraphHandler(api.GraphAdminConfig{
			Projects:      projectRepo,
			Entities:      entityRepo,
			Relationships: relationshipRepo,
			Aliases:       entityAliasRepo,
			Namespaces:    namespaceRepo,
			Orgs:          orgRepo,
			Settings:      settingsSvc,
		}),
		AdminDreaming: api.NewAdminDreamingHandler(api.DreamAdminConfig{
			Store:    dreamAdminStore,
			Rollback: dreamRollback,
		}),

		// Tier-B (org-aggregate) handlers — caller must be RoleOrgOwner+
		// of the org passed in {org_id}. Aggregate counts + distributions
		// only.
		OrgDashboard: api.NewOrgDashboardHandler(api.OrgDashboardConfig{
			Store: aggregatesStore,
			Audit: auditStore,
		}),
		OrgActivity: api.NewOrgActivityHandler(api.OrgDashboardConfig{
			Store: aggregatesStore,
			Audit: auditStore,
		}),
		OrgAnalytics: api.NewOrgAnalyticsHandler(api.OrgAnalyticsConfig{
			Store: aggregatesStore,
		}),
		OrgUsage: api.NewAdminUsageHandler(api.UsageConfig{Store: usageStore}),

		// Tier-C (system-aggregate) handlers — RoleAdministrator only via
		// the /v1/admin route group gate. System totals + per-org rows;
		// no per-user, no per-memory, no content.
		SystemDashboard: api.NewSystemDashboardHandler(api.SystemDashboardConfig{
			Store: aggregatesStore,
			Audit: auditStore,
		}),
		SystemActivity: api.NewSystemActivityHandler(api.SystemDashboardConfig{
			Store: aggregatesStore,
			Audit: auditStore,
		}),
		SystemAnalytics: api.NewSystemAnalyticsHandler(api.SystemAnalyticsConfig{
			Store: aggregatesStore,
		}),
		SystemUsage: api.NewAdminUsageHandler(api.UsageConfig{Store: usageStore}),
	}

	// Build router config with auth middleware and rate limiter. Cleanup
	// and stale-after windows are read once from settings; runtime changes
	// require server restart.
	authMiddleware := auth.NewAuthMiddleware(apiKeyRepo, userRepo, jwtSecret)
	rateLimiter := auth.NewRateLimiter(10, 20,
		settingsSvc.ResolveDurationSecondsWithDefault(context.Background(),
			service.SettingAPIRateLimitCleanupSeconds, "global"),
		settingsSvc.ResolveDurationSecondsWithDefault(context.Background(),
			service.SettingAPIRateLimitStaleSeconds, "global"))
	defer rateLimiter.Stop()

	// Project access middleware enforces org-membership checks on all
	// /v1/projects/{project_id}/memories/* routes.
	projectAccessCfg := api.ProjectAccessConfig{
		Projects:   projectRepo,
		Namespaces: namespaceRepo,
		Orgs:       orgRepo,
		Users:      userRepo,
	}

	routerCfg := server.RouterConfig{
		Metrics:        metrics,
		AuthMiddleware: authMiddleware,
		RateLimiter:    rateLimiter,
		SetupGuard:     api.SetupGuardMiddleware(setupChecker.IsComplete),
		ProjectAccess:  api.ProjectAccessMiddleware(projectAccessCfg),
		EnrichmentGate: api.EnrichmentGateMiddleware(enrichmentAvailable),
	}

	r := server.NewRouter(routerCfg, handlers)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("server starting on %s (log_level=%s)", addr, cfg.LogLevel)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed to start: %v", err)
		}
	}()

	<-done
	log.Println("server shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}

	log.Println("server stopped")
}

// seedRegisteredSettings inserts one row per registered schema entry, using
// INSERT ... ON CONFLICT DO NOTHING so operator-set values survive untouched.
// Idempotent on every boot — every key the admin UI surfaces has a row.
func seedRegisteredSettings(ctx context.Context, repo *storage.SettingsRepo) error {
	schemas := adminstore.SettingsSchemas()
	defaults := make(map[string]string, len(schemas))
	for _, s := range schemas {
		defaults[s.Key] = string(s.DefaultValue)
	}
	return service.SeedSettingsDefaults(ctx, repo, defaults)
}

// loadRankingWeights pulls the ranking.weight.* settings into a RankingWeights.
// Each lookup falls back to the corresponding DefaultRankingWeights field when
// the key is missing, unparseable, or out of range, so a misconfigured setting
// keeps ranking in a known state rather than crashing startup. Per-project
// overrides resolve later at recall time.
func loadRankingWeights(ctx context.Context, s *service.SettingsService) service.RankingWeights {
	d := service.DefaultRankingWeights
	return service.RankingWeights{
		Similarity:     s.ResolveFloatInRange(ctx, service.SettingRankWeightSim, "global", 0, 1, d.Similarity),
		Recency:        s.ResolveFloatInRange(ctx, service.SettingRankWeightRec, "global", 0, 1, d.Recency),
		Importance:     s.ResolveFloatInRange(ctx, service.SettingRankWeightImp, "global", 0, 1, d.Importance),
		Frequency:      s.ResolveFloatInRange(ctx, service.SettingRankWeightFreq, "global", 0, 1, d.Frequency),
		GraphRelevance: s.ResolveFloatInRange(ctx, service.SettingRankWeightGraph, "global", 0, 1, d.GraphRelevance),
		Confidence:     s.ResolveFloatInRange(ctx, service.SettingRankWeightConf, "global", 0, 1, d.Confidence),
	}
}

// loadFusionConfig pulls the recall.fusion.* settings into a FusionConfig.
// Each lookup falls back to the registered default when the key is missing
// or unparseable, so a misconfigured setting keeps fusion in a known state
// rather than crashing startup.
func loadFusionConfig(ctx context.Context, settingsSvc *service.SettingsService) service.FusionConfig {
	cfg := service.DefaultFusionConfig
	cfg.Enabled = settingsSvc.ResolveBool(ctx, service.SettingRecallFusionEnabled, "global")
	if k, err := settingsSvc.ResolveInt(ctx, service.SettingRecallFusionK, "global"); err == nil && k > 0 {
		cfg.RRFConstant = k
	}
	if w, err := settingsSvc.ResolveFloat(ctx, service.SettingRecallFusionVecW, "global"); err == nil && w >= 0 {
		cfg.VectorWeight = w
	}
	if w, err := settingsSvc.ResolveFloat(ctx, service.SettingRecallFusionLexW, "global"); err == nil && w >= 0 {
		cfg.LexicalWeight = w
	}
	return cfg
}
