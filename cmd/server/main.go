package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/config"
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

	// Create repositories.
	memoryRepo := storage.NewMemoryRepo(db)
	projectRepo := storage.NewProjectRepo(db)
	namespaceRepo := storage.NewNamespaceRepo(db)

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

	// Create provider registry.
	// First try config file values, then overlay with DB-persisted settings
	// (providers configured via admin UI are stored in the settings table).
	var registry *provider.Registry
	regCfg := provider.RegistryConfig{
		Embedding: provider.SlotConfig{
			Type:    cfg.Embed.Provider,
			BaseURL: cfg.Embed.URL,
			APIKey:  cfg.Embed.Key,
			Model:   cfg.Embed.Model,
		},
		Fact: provider.SlotConfig{
			Type:    cfg.Fact.Provider,
			BaseURL: cfg.Fact.URL,
			APIKey:  cfg.Fact.Key,
			Model:   cfg.Fact.Model,
		},
		Entity: provider.SlotConfig{
			Type:    cfg.Entity.Provider,
			BaseURL: cfg.Entity.URL,
			APIKey:  cfg.Entity.Key,
			Model:   cfg.Entity.Model,
		},
	}

	// Overlay DB-persisted provider settings (from admin UI) on top of config file.
	dbSlots := []struct {
		key  string
		dest *provider.SlotConfig
	}{
		{"provider.embedding", &regCfg.Embedding},
		{"provider.fact", &regCfg.Fact},
		{"provider.entity", &regCfg.Entity},
	}
	for _, slot := range dbSlots {
		setting, sErr := settingsRepo.Get(context.Background(), slot.key, "global")
		if sErr != nil {
			continue
		}
		var apiCfg api.ProviderSlotConfig
		if json.Unmarshal(setting.Value, &apiCfg) == nil && apiCfg.Type != "" {
			slot.dest.Type = apiCfg.Type
			slot.dest.BaseURL = apiCfg.URL
			if apiCfg.APIKey != "" {
				slot.dest.APIKey = apiCfg.APIKey
			}
			slot.dest.Model = apiCfg.Model
		}
	}

	registry, err = provider.NewRegistry(regCfg)
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

	// Overlay DB-persisted Qdrant settings on top of config file values.
	qdrantKeys := []struct {
		key   string
		apply func(string)
	}{
		{service.SettingQdrantAddr, func(v string) { cfg.Qdrant.Addr = v }},
		{service.SettingQdrantAPIKey, func(v string) { cfg.Qdrant.APIKey = v }},
		{service.SettingQdrantUseTLS, func(v string) { cfg.Qdrant.UseTLS = v == "true" }},
		{service.SettingQdrantPoolSize, func(v string) {
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				cfg.Qdrant.PoolSize = uint(n)
			}
		}},
		{service.SettingQdrantKeepAliveTime, func(v string) {
			if n, err := strconv.Atoi(v); err == nil {
				cfg.Qdrant.KeepAliveTime = n
			}
		}},
		{service.SettingQdrantKeepAliveTimeout, func(v string) {
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				cfg.Qdrant.KeepAliveTimeout = uint(n)
			}
		}},
	}
	for _, qk := range qdrantKeys {
		setting, sErr := settingsRepo.Get(context.Background(), qk.key, "global")
		if sErr != nil {
			continue
		}
		var val string
		if json.Unmarshal(setting.Value, &val) != nil {
			val = string(setting.Value)
		}
		if val != "" {
			qk.apply(val)
		}
	}

	// Create vector store.
	// Priority: Qdrant (if configured) > PgVector (if Postgres) > HNSWStore (if SQLite).
	var vectorStore storage.VectorStore
	if cfg.Qdrant.Addr != "" {
		vectorStore, err = storage.NewQdrantStore(cfg.Qdrant)
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
			M:                cfg.HNSW.M,
			EfConstruction:   cfg.HNSW.EfConstruction,
			EfSearch:         cfg.HNSW.EfSearch,
			MaxLoadedIndexes: cfg.HNSW.MaxLoadedIndexes,
		}
		hnswStore := storage.NewHNSWStore(db.DB(), db.WriteDB(), hnswCfg)
		vectorStore = hnswStore
		defer hnswStore.Close()
		log.Println("hnsw vector store initialized (SQLite backend)")
	}

	// Create event bus.
	eventBus := events.NewEventBus(db.Backend(), nil)
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
		ingestionLogRepo, tokenUsageRepo, enrichmentQueueRepo,
		vectorStore, embedProvider,
	)
	recallSvc := service.NewRecallService(
		memoryRepo, projectRepo, namespaceRepo,
		tokenUsageRepo, vectorStore, entityRepo,
		relationshipRepo, shareRepo, embedProvider,
	)
	updateSvc := service.NewUpdateService(
		memoryRepo, projectRepo, lineageRepo,
		vectorStore, tokenUsageRepo, embedProvider,
	)
	forgetSvc := service.NewForgetService(
		memoryRepo, projectRepo, vectorStore,
		relationshipRepo, lineageRepo, enrichmentQueueRepo, tokenUsageRepo,
	)
	batchGetSvc := service.NewBatchGetService(memoryRepo, projectRepo)
	batchStoreSvc := service.NewBatchStoreService(
		memoryRepo, projectRepo, namespaceRepo,
		ingestionLogRepo, tokenUsageRepo, enrichmentQueueRepo,
		vectorStore, embedProvider,
	)
	enrichSvc := service.NewEnrichService(memoryRepo, projectRepo, enrichmentQueueRepo)
	exportSvc := service.NewExportService(
		memoryRepo, entityRepo, relationshipRepo, lineageRepo, projectRepo,
	)
	importSvc := service.NewImportService(
		memoryRepo, projectRepo, namespaceRepo, ingestionLogRepo,
	)
	_ = service.NewSettingsService(settingsRepo)

	// Create lifecycle service for TTL expiry and purge sweeps.
	graphPruner := service.NewGraphPruner(entityRepo, relationshipRepo)
	lifecycleSvc := service.NewLifecycleService(memoryRepo, vectorStore, graphPruner, service.LifecycleConfig{})
	lifecycleSvc.Start()
	defer lifecycleSvc.Stop()

	// Create MCP server.
	mcpServer := mcp.NewServer(mcp.Dependencies{
		Backend:       db.Backend(),
		Store:         storeSvc,
		Recall:        recallSvc,
		Forget:        forgetSvc,
		Update:        updateSvc,
		BatchGet:      batchGetSvc,
		BatchStore:    batchStoreSvc,
		Enrich:        enrichSvc,
		Export:        exportSvc,
		ProjectRepo:   projectRepo,
		UserRepo:      userRepo,
		NamespaceRepo: namespaceRepo,
		EntityReader:  entityRepo,
		Traverser:     relationshipRepo,
		EventBus:      eventBus,
		ProviderStatus: func() (bool, bool) {
			if registry == nil {
				return false, false
			}
			hasEmbed := registry.GetEmbedding() != nil
			hasEnrich := registry.GetFact() != nil && registry.GetEntity() != nil
			return hasEmbed, hasEnrich
		},
	})

	// Create metrics.
	metrics := api.NewMetrics()

	// Build start time for health handler.
	startTime := time.Now()

	// Create setup checker (cached atomic bool, queries DB once).
	setupChecker := api.NewSetupChecker(db)

	// Create admin store adapters.
	setupStore := adminstore.NewSetupStore(userRepo, namespaceRepo, orgRepo, apiKeyRepo, projectRepo, db)
	orgAdminStore := adminstore.NewOrgAdminStore(orgRepo, namespaceRepo)
	userAdminStore := adminstore.NewUserAdminStore(userRepo, apiKeyRepo, namespaceRepo, orgRepo, projectRepo)
	projectAdminStore := adminstore.NewProjectAdminStore(db, projectRepo, namespaceRepo)
	webhookAdminStore := adminstore.NewWebhookAdminStore(webhookRepo)
	settingsAdminStore := adminstore.NewSettingsAdminStore(settingsRepo)
	dashboardStore := adminstore.NewDashboardStore(db, enrichmentQueueRepo)
	analyticsStore := adminstore.NewAnalyticsStore(db)
	usageStore := adminstore.NewUsageStore(db)
	databaseAdminStore := adminstore.NewDatabaseAdminStore(db)
	namespaceAdminStore := adminstore.NewNamespaceAdminStore(db)
	providerAdminStore := adminstore.NewProviderAdminStore(registry, settingsRepo)
	oauthAdminStore := adminstore.NewOAuthAdminStore(oauthRepo)
	enrichmentAdminStore := adminstore.NewEnrichmentAdminStore(enrichmentQueueRepo, settingsRepo, db)

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

	// Start enrichment worker pool — needs providers for LLM extraction.
	workerPool := enrichment.NewWorkerPool(
		enrichment.WorkerConfig{Backend: db.Backend()},
		memoryRepo, memoryRepo, memoryRepo, enrichmentQueueRepo,
		entityRepo, relationshipRepo, lineageRepo, tokenUsageRepo, vectorStore,
		factProvider, entityProvider, embedProvider,
	)
	workerPool.Start()
	defer workerPool.Stop()
	log.Println("enrichment worker pool started")

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
		UserRepo:  userRepo,
		JWTSecret: jwtSecret,
	}

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
		List:       api.NewListHandler(memoryRepo, projectRepo),
		Detail:     api.NewDetailHandler(memoryRepo, projectRepo),
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
		MeAPIKeys:           api.NewMeAPIKeysHandler(apiKeyRepo),
		MeAPIKeyRevoke:      api.NewMeAPIKeyRevokeHandler(apiKeyRepo),
		MeOAuthClients:      api.NewMeOAuthClientsHandler(oauthRepo),
		MeOAuthClientRevoke: api.NewMeOAuthClientRevokeHandler(oauthRepo),
		MeChangePassword:    api.NewMeChangePasswordHandler(userRepo),

		// Org-scoped handlers
		OrgUsers: api.NewOrgUsersHandler(api.OrgUserConfig{Store: userAdminStore}),
		OrgIdP:   api.NewOrgIdPHandler(oauthRepo),

		// SSE events
		Events: api.NewEventsHandler(eventBus),

		// MCP server
		MCP: mcpServer.Handler(),

		// Embedded admin UI
		UI: ui.Handler(),

		// Auth handlers
		AuthLogin:  api.NewLoginHandler(authCfg),
		AuthLookup: api.NewLookupHandler(authCfg),

		// OAuth handlers
		OAuthAuthorize:         oauthServer.AuthorizeHandler(),
		OAuthToken:             oauthServer.TokenHandler(),
		OAuthRegister:          oauthServer.RegisterClientHandler(),
		OAuthUserInfo:          oauthServer.UserInfoHandler(),
		OAuthMetadata:          oauthServer.MetadataHandler(),
		OAuthProtectedResource: oauthServer.ProtectedResourceHandler(),

		// Admin handlers
		AdminSetupStatus: api.NewAdminSetupStatusHandler(api.SetupConfig{Store: setupStore}),
		AdminSetup: api.NewAdminSetupHandler(api.SetupConfig{
			Store:      setupStore,
			JWTSecret:  jwtSecret,
			OnComplete: setupChecker.MarkComplete,
		}),
		AdminDashboard:   api.NewAdminDashboardHandler(api.DashboardConfig{Store: dashboardStore}),
		AdminActivity:    api.NewAdminActivityHandler(api.DashboardConfig{Store: dashboardStore}),
		AdminOrgs:        api.NewAdminOrgsHandler(api.OrgAdminConfig{Store: orgAdminStore}),
		AdminUsers:       api.NewAdminUsersHandler(api.UserAdminConfig{Store: userAdminStore}),
		AdminProjects:    api.NewAdminProjectsHandler(api.ProjectAdminConfig{Store: projectAdminStore}),
		AdminProviders:   api.NewAdminProvidersHandler(api.ProviderAdminConfig{Store: providerAdminStore}),
		AdminSettings:    api.NewAdminSettingsHandler(api.SettingsAdminConfig{Store: settingsAdminStore}),
		AdminEnrichment: api.NewAdminEnrichmentHandler(api.EnrichmentAdminConfig{
			Store:          enrichmentAdminStore,
			FactProvider:   factProvider,
			EntityProvider: entityProvider,
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
		}),
	}

	// Build router config with auth middleware and rate limiter.
	authMiddleware := auth.NewAuthMiddleware(apiKeyRepo, userRepo, jwtSecret)
	rateLimiter := auth.NewRateLimiter(10, 20)
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
