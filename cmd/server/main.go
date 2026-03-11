package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"crypto/rand"
	"fmt"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/config"
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
	handled, err := migration.RunCLI(os.Args, db.DB(), db.Backend())
	if err != nil {
		log.Fatalf("migration command failed: %v", err)
	}
	if handled {
		return
	}

	// Auto-migrate on startup if configured.
	if cfg.Database.MigrateOnStart {
		m, err := migration.NewMigrator(db.DB(), db.Backend())
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

	// Create provider registry (may have no providers configured).
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

	// Create vector store (Qdrant if configured, otherwise nil for SQLite).
	var vectorStore storage.VectorStore
	if cfg.Qdrant.Addr != "" {
		vectorStore, err = storage.NewQdrantStore(cfg.Qdrant.Addr)
		if err != nil {
			log.Printf("warning: qdrant connection failed (vector search disabled): %v", err)
		}
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
	lifecycleSvc := service.NewLifecycleService(memoryRepo, vectorStore, service.LifecycleConfig{})
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
		OrgRepo:       orgRepo,
		EntityReader:  entityRepo,
		Traverser:     relationshipRepo,
		EventBus:      eventBus,
	})

	// Create metrics.
	metrics := api.NewMetrics()

	// Build start time for health handler.
	startTime := time.Now()

	// Create admin store adapters.
	setupStore := adminstore.NewSetupStore(userRepo, namespaceRepo, orgRepo, apiKeyRepo, db)
	orgAdminStore := adminstore.NewOrgAdminStore(orgRepo, namespaceRepo)
	userAdminStore := adminstore.NewUserAdminStore(userRepo, apiKeyRepo, namespaceRepo, orgRepo)
	projectAdminStore := adminstore.NewProjectAdminStore(projectRepo, namespaceRepo)
	webhookAdminStore := adminstore.NewWebhookAdminStore(webhookRepo)
	settingsAdminStore := adminstore.NewSettingsAdminStore(settingsRepo)
	dashboardStore := adminstore.NewDashboardStore(db, enrichmentQueueRepo)
	analyticsStore := adminstore.NewAnalyticsStore(db)
	usageStore := adminstore.NewUsageStore(db)
	databaseAdminStore := adminstore.NewDatabaseAdminStore(db)
	namespaceAdminStore := adminstore.NewNamespaceAdminStore(db)
	providerAdminStore := adminstore.NewProviderAdminStore(registry, settingsRepo)
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

		// Org-scoped handlers
		OrgRecall: api.NewOrgRecallHandler(recallSvc, orgRepo),

		// SSE events
		Events: api.NewEventsHandler(eventBus),

		// MCP server
		MCP: mcpServer.Handler(),

		// Embedded admin UI
		UI: ui.Handler(),

		// Admin handlers
		AdminSetupStatus: api.NewAdminSetupStatusHandler(api.SetupConfig{Store: setupStore}),
		AdminSetup:       api.NewAdminSetupHandler(api.SetupConfig{Store: setupStore}),
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
		}),
	}

	// Generate JWT secret (random 32 bytes per process — in production this
	// should be persisted or configured, but for now auto-generate is fine).
	jwtSecret := make([]byte, 32)
	if _, err := rand.Read(jwtSecret); err != nil {
		log.Fatalf("failed to generate jwt secret: %v", err)
	}

	// Build router config with auth middleware and rate limiter.
	authMiddleware := auth.NewAuthMiddleware(apiKeyRepo, jwtSecret)
	rateLimiter := auth.NewRateLimiter(10, 20)
	defer rateLimiter.Stop()

	routerCfg := server.RouterConfig{
		Metrics:        metrics,
		AuthMiddleware: authMiddleware,
		RateLimiter:    rateLimiter,
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
