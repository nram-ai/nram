package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/dreaming"
	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/logging"
	"github.com/nram-ai/nram/internal/mcp"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/observability/metrics"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/server"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
	adminstore "github.com/nram-ai/nram/internal/storage/admin"
	"github.com/nram-ai/nram/internal/ui"
	"github.com/nram-ai/nram/internal/version"
)

// runHeadlessBootstrap creates the first administrator from the bootstrap
// admin credentials when the database is empty. Returns true when setup is
// complete after the call (either it already was, or this call just made it
// so), letting the caller seed the cached SetupChecker without re-querying
// the DB. Idempotent: re-running with the same credentials is safe.
func runHeadlessBootstrap(ctx context.Context, store *adminstore.SetupStore, adminCfg config.AdminConfig) bool {
	complete, err := store.IsSetupComplete(ctx)
	if err != nil {
		slog.Warn("boot: headless bootstrap setup-status check failed", "err", err)
		return false
	}
	if complete {
		return true
	}

	switch {
	case adminCfg.Email == "" && adminCfg.Password == "":
		return false
	case adminCfg.Email == "" || adminCfg.Password == "":
		slog.Warn("boot: headless bootstrap skipped, both admin.email and admin.password (or NRAM_ADMIN_EMAIL/NRAM_ADMIN_PASS) must be set")
		return false
	}

	user, _, err := store.CompleteSetup(ctx, adminCfg.Email, adminCfg.Password)
	if err != nil {
		slog.Error("boot: headless bootstrap failed to create administrator", "email", adminCfg.Email, "err", err)
		return false
	}
	slog.Info("boot: headless bootstrap created administrator", "email", user.Email, "id", user.ID)
	return true
}

// configureLogger installs a slog text handler at the level named by
// cfg.LogLevel (info|debug|warn|error) and returns it. Without this, slog
// defaults to INFO and the cfg.LogLevel field is read but never honoured. The
// returned console handler is later wrapped by the fanout handler so diagnostic
// logs also reach the log_entries table; until then it is the default so any
// pre-database boot logging still prints.
func configureLogger(level string) slog.Handler {
	console := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logging.ParseLevel(level),
	})
	slog.SetDefault(slog.New(console))
	return console
}

func main() {
	// Answer --help/-h, --version/-v, and per-subcommand help before touching
	// the config file or database, so they work in any environment.
	if handleInfoFlags(os.Args) {
		return
	}

	// Honour --workdir before any CWD-relative read (config.yaml, ./nram.db) so
	// a service manager launching the binary from an arbitrary directory still
	// resolves files from the directory captured at install time.
	if werr := applyWorkdir(os.Args); werr != nil {
		log.Fatalf("%v", werr)
	}

	// Register/unregister/control the binary as a native OS service and exit.
	// Routed before config.Load, like the other one-shot commands, because it
	// talks only to the OS service manager and never needs the database.
	if len(os.Args) > 1 && os.Args[1] == "service" {
		if serr := dispatchServiceCommand(os.Args); serr != nil {
			log.Fatalf("%v", serr)
		}
		return
	}

	// Determine config file path from --config flag if provided.
	cfg, err := config.Load(configPathFromArgs(os.Args))
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	consoleHandler := configureLogger(cfg.LogLevel)

	db, err := storage.Open(cfg.Database)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	slog.Info("boot: database backend", "backend", db.Backend())

	// Handle the SQLite-to-Postgres data migration before starting the server.
	// It lives here, not in internal/migration, because it delegates to the
	// adminstore DataMigrator (conflict-safe, type-aware, copies every table
	// including vectors and the graph). internal/storage/admin imports
	// internal/migration, so internal/migration cannot import it back; main is
	// the seam that can reach both.
	if len(os.Args) > 1 && os.Args[1] == "migrate-to-postgres" {
		_, targetURL, perr := migration.ParseMigrateArgs(os.Args)
		if perr != nil || targetURL == "" {
			log.Fatalf("%s", migrateToPostgresUsage)
		}
		status, merr := adminstore.NewDatabaseAdminStore(db, nil).TriggerMigration(context.Background(), targetURL)
		if merr != nil {
			log.Fatalf("migrate-to-postgres failed: %v", merr)
		}
		if status.Status != "complete" {
			log.Fatalf("migrate-to-postgres: %s", status.Message)
		}
		slog.Info("boot: migrate-to-postgres complete", "status", status.Message)
		return
	}

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
		_ = m.Close()
		slog.Info("boot: migrations applied successfully")
	}

	for _, arg := range os.Args[1:] {
		if arg == "--backfill-enrichment" {
			n, err := storage.EnqueueUncoveredMemories(context.Background(), db)
			if err != nil {
				log.Fatalf("enrichment backfill failed: %v", err)
			}
			slog.Info("boot: enrichment backfill enqueued jobs", "count", n)
			return
		}
		if arg == "--reembed-all-memories" {
			n, err := storage.EnqueueAllLiveMemories(context.Background(), db)
			if err != nil {
				log.Fatalf("reembed all memories failed: %v", err)
			}
			slog.Info("boot: reembed enqueued memory re-embed jobs (force, every live memory)", "count", n)
			return
		}
		if arg == "--normalize-memory-tags" {
			n, err := storage.NormalizeMemoryTags(context.Background(), db)
			if err != nil {
				log.Fatalf("normalize memory tags failed: %v", err)
			}
			slog.Info("boot: normalize-memory-tags rewrote tags on memory rows", "rows", n)
			return
		}
	}

	// Create repositories.
	memoryRepo := storage.NewMemoryRepo(db)
	projectRepo := storage.NewProjectRepo(db)
	namespaceRepo := storage.NewNamespaceRepo(db)
	proceduralRepo := storage.NewProceduralRepo(db)

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
			slog.Info("boot: content_hash backfill populated rows", "rows", total)
		}
	}

	// Ensure every user has the reserved projects (global + about_me) and that
	// their canonical Name/Description are healed. This is idempotent: existing
	// reserved projects are reused and only repaired if drifted. Handles upgrades
	// from versions before about_me, and before global carried a description.
	{
		tmpUserRepo := storage.NewUserRepo(db)
		users, err := tmpUserRepo.ListAll(context.Background())
		if err == nil {
			for _, u := range users {
				if err := projectRepo.EnsureReservedUnderUser(context.Background(), namespaceRepo, u.NamespaceID); err != nil {
					slog.Warn("boot: ensure reserved projects failed", "user", u.ID, "err", err)
				}
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
	webhookRepo := storage.NewWebhookRepo(db)
	ingestionLogRepo := storage.NewIngestionLogRepo(db)
	tokenUsageRepo := storage.NewTokenUsageRepo(db)
	enrichmentQueueRepo := storage.NewEnrichmentQueueRepo(db)
	exportJobRepo := storage.NewExportJobRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	settingsSvc := service.NewSettingsService(settingsRepo)

	// Seed registered defaults at boot so SELECT key FROM settings reflects
	// the full schema surface area instead of the subset operators have
	// touched. Idempotent; never overwrites operator-set values.
	if err := seedRegisteredSettings(context.Background(), settingsRepo); err != nil {
		slog.Warn("boot: settings seed failed", "err", err)
	}

	// Load (or generate on first boot) this deployment's persistent instance
	// identity: a v4 UUID plus an ECDSA P-256 signing keypair, both stored in
	// system_meta and never settable through the UI. Generated once and loaded
	// verbatim thereafter; it survives restarts and a SQLite->Postgres data
	// migration (DataMigrator copies system_meta verbatim). The private half
	// signs on the instance's behalf; the public half is exposed read-only.
	instanceIdentity, err := storage.LoadOrCreateInstanceIdentity(context.Background(), db)
	if err != nil {
		log.Fatalf("failed to load instance identity: %v", err)
	}
	slog.Info("boot: instance identity", "instance_id", instanceIdentity.ID)

	// Install the SQL log sink. From here on, slog (and, via the bridge below,
	// the stdlib log package) tees diagnostic logs to the log_entries table for
	// the operator Logs page, in addition to the unchanged console output. The
	// DB capture toggle and level are seeded from settings and refreshed live by
	// a goroutine started further down, so admin changes apply without a restart.
	// Done after the settings seed so the logging.* defaults exist.
	logEntryRepo := storage.NewLogEntryRepo(db)
	logDBConfig := logging.NewDBConfig(
		settingsSvc.ResolveBoolWithDefault(context.Background(), service.SettingLoggingDBCaptureEnabled, "global"),
		logging.ParseLevel(settingsSvc.ResolveStringWithDefault(context.Background(), service.SettingLoggingDBLevel, "global")),
	)
	logWriter := logging.NewAsyncWriter(logging.NewSQLSink(logEntryRepo), logging.WriterOptions{})
	logWriter.Start()
	slog.SetDefault(slog.New(logging.NewFanoutHandler(consoleHandler, logDBConfig, logWriter)))
	// Capture any remaining stdlib log output through the same pipeline.
	log.SetOutput(logging.StdBridge())
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = logWriter.Close(flushCtx)
	}()

	// Refresh the log sink's enabled flag and level from settings periodically so
	// admin changes to logging.db_capture_enabled / logging.db_level take effect
	// without a restart.
	logRefreshCtx, logRefreshCancel := context.WithCancel(context.Background())
	defer logRefreshCancel()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-logRefreshCtx.Done():
				return
			case <-ticker.C:
				logDBConfig.SetEnabled(settingsSvc.ResolveBoolWithDefault(logRefreshCtx, service.SettingLoggingDBCaptureEnabled, "global"))
				logDBConfig.SetLevel(logging.ParseLevel(settingsSvc.ResolveStringWithDefault(logRefreshCtx, service.SettingLoggingDBLevel, "global")))
			}
		}
	}()

	// Start the log-retention sweeper: prune the log_entries rolling window to
	// the configured row cap (hard ceiling) and age limit, resolved live each
	// pass. Runs hourly in its own goroutine.
	logRetention := logging.NewRetentionSweeper(logEntryRepo, func(ctx context.Context) logging.RetentionLimits {
		return logging.RetentionLimits{
			MaxRows:    settingsSvc.ResolveIntWithDefault(ctx, service.SettingLoggingRetentionMaxRows, "global"),
			MaxAgeDays: settingsSvc.ResolveIntWithDefault(ctx, service.SettingLoggingRetentionMaxAge, "global"),
		}
	})
	logRetentionCtx, logRetentionCancel := context.WithCancel(context.Background())
	defer logRetentionCancel()
	go logRetention.Run(logRetentionCtx, time.Hour)

	// One-time canonicalization of existing relationships.relation values so rows
	// written before write-time canonicalization (and the admin graph viz, which
	// reads stored relations verbatim) are clean and merged. Guarded by a marker
	// setting so the table is scanned at most once; the pass is idempotent and not
	// load-bearing (read-time dedup covers responses), so a failure logs and
	// retries next boot rather than blocking startup.
	{
		const relCanonFlag = "relationships.canonicalized"
		ctx := context.Background()
		if !settingsSvc.ResolveBool(ctx, relCanonFlag, "global") {
			changed, err := migration.CanonicalizeRelations(ctx, db.WriteDB(), db.Backend())
			if err != nil {
				slog.Warn("boot: relation canonicalization backfill failed (will retry next boot)", "err", err)
			} else {
				if changed > 0 {
					slog.Info("boot: relation canonicalization normalized/merged relationship rows", "rows", changed)
				}
				if err := settingsSvc.Set(ctx, relCanonFlag, "true", "global", nil); err != nil {
					slog.Warn("boot: failed to record relation canonicalization marker", "err", err)
				}
			}
		}
	}

	// One-time split of a stored entity_system_prompt that still holds the old
	// combined entities+relationships default, so existing deployments pick up the
	// entity-only prompt (relationships now extract in a separate pass). Guarded by
	// a marker; an operator-customized prompt is left untouched with a warning.
	migrateEntityPromptSplit(context.Background(), settingsSvc)

	// Create provider registry. Provider configuration lives in the DB
	// settings table (provider.{embedding,fact,entity}) and is managed via
	// the admin UI. On a fresh install the slots are empty and the registry
	// reports providers unavailable until an admin completes setup.
	// Create Prometheus metrics before the provider registry so the token
	// counter is wired in at construction time. Tests leave the metrics
	// sink nil and the recording sites no-op.
	promMetrics := metrics.New()

	// Normalize legacy provider type values (pre-0.5.4 "custom" ->
	// "openai-compatible") in the settings table before loading the registry so
	// the persisted rows match the canonical names the UI now sends. Idempotent;
	// a failure is non-fatal because the read path also normalizes on the fly.
	if n, mErr := adminstore.MigrateProviderTypes(context.Background(), settingsRepo); mErr != nil {
		slog.Warn("boot: provider type migration failed", "err", mErr)
	} else if n > 0 {
		slog.Info("boot: migrated legacy provider types", "slots", n)
	}

	regCfg := adminstore.LoadProviderRegistryConfig(context.Background(), settingsRepo)
	registry, err := provider.NewRegistry(regCfg, tokenUsageRepo, namespaceRepo)
	if err != nil {
		slog.Warn("boot: provider registry init failed, providers disabled", "err", err)
		registry = nil
	}
	if registry != nil {
		// Install the metrics hooks. The Registry's wrap funcs read these
		// through atomic.Pointers via indirect closures, so the hooks
		// survive future Reload calls without re-wrapping.
		//
		// The embed wrapper sits INSIDE the usage recorder (see
		// registry.wrapEmbedding) so nram_embedding_duration_seconds
		// measures only the upstream provider call, not the synchronous
		// token_usage DB write.
		registry.WithTokenCounter(func(p, op string, n float64) {
			promMetrics.TokensUsedTotal.WithLabelValues(p, op).Add(n)
		})
		registry.WithEmbeddingWrapper(func(ep provider.EmbeddingProvider) provider.EmbeddingProvider {
			return metrics.WrapEmbeddingProvider(ep, promMetrics)
		})
		// Observe embedding-cache hit/miss so the Metrics page can show the
		// hit rate. Fired by the cache wrapper, which sits outside the usage
		// recorder, so a hit increments this without landing a token_usage row.
		registry.WithEmbedCacheCounter(func(hit bool, n int) {
			result := "miss"
			if hit {
				result = "hit"
			}
			promMetrics.EmbeddingCacheLookups.WithLabelValues(result).Add(float64(n))
		})
		// Install the exact-match embedding cache (outermost, so a full hit
		// records no token_usage row). Config is read live from settings on
		// every Embed, so the admin toggle and size/TTL knobs take effect
		// without a restart.
		registry.WithEmbeddingCache(func(ctx context.Context) provider.EmbedCacheConfig {
			return provider.EmbedCacheConfig{
				Enabled:    settingsSvc.ResolveBool(ctx, service.SettingEmbeddingCacheEnabled, "global"),
				MaxEntries: settingsSvc.ResolveIntWithDefault(ctx, service.SettingEmbeddingCacheMaxEntries, "global"),
				TTL:        time.Duration(settingsSvc.ResolveIntWithDefault(ctx, service.SettingEmbeddingCacheTTLSeconds, "global")) * time.Second,
			}
		})
		// Install the shared host-keyed concurrency gate. The limits are read
		// live from settings before every gated call (served from the settings
		// cache), so an admin raising or lowering them takes effect within the
		// cache TTL with no restart. Bounds aggregate in-flight requests per
		// upstream host across every worker slot and subsystem, so a saturated
		// worker pool cannot overwhelm a single shared model/embed host.
		registry.WithHostConcurrency(func(ctx context.Context) provider.HostConcurrency {
			return provider.HostConcurrency{
				LLM:   settingsSvc.ResolveIntWithDefault(ctx, service.SettingProviderLLMHostConcurrency, "global"),
				Embed: settingsSvc.ResolveIntWithDefault(ctx, service.SettingProviderEmbedHostConcurrency, "global"),
			}
		})
		// Install the live circuit-breaker thresholds. Read from the settings
		// cache on each breaker state evaluation, so an admin changing the
		// backoff knobs takes effect within the cache TTL with no restart. The
		// breaker itself has no request context, so resolve against Background;
		// these are global-scope settings.
		registry.WithCircuitBreaker(func() provider.CircuitBreakerBounds {
			ctx := context.Background()
			return provider.CircuitBreakerBounds{
				MaxFailures: settingsSvc.ResolveIntWithDefault(ctx, service.SettingProviderCircuitBreakerMaxFailures, "global"),
				ResetBase:   settingsSvc.ResolveDurationSecondsWithDefault(ctx, service.SettingProviderCircuitBreakerResetBaseSeconds, "global"),
				ResetMax:    settingsSvc.ResolveDurationSecondsWithDefault(ctx, service.SettingProviderCircuitBreakerResetMaxSeconds, "global"),
			}
		})
		// Reload so the embedding provider already wrapped by NewRegistry
		// picks up the freshly-installed embed wrapper, cache, and host gate.
		// On configs with no embedding slot this is a no-op.
		if rerr := registry.Reload(regCfg); rerr != nil {
			slog.Warn("boot: registry reload to install metrics hooks failed", "err", rerr)
		}
	}

	// embedProvider returns the live embedding provider from the registry.
	// The metrics wrap is now installed via the registry's EmbeddingWrapper
	// hook above, so no wrapping happens here per call.
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

	// buildHNSWConfig resolves the deployment's HNSW tuning from settings. Used
	// both to construct the live SQLite vector store and the migration store's
	// reverse-write target, so they stay in lockstep.
	buildHNSWConfig := func() storage.HNSWConfig {
		return storage.HNSWConfig{
			M:                settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingHNSWM, "global"),
			EfConstruction:   settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingHNSWEfConstruction, "global"),
			EfSearch:         settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingHNSWEfSearch, "global"),
			MaxLoadedIndexes: settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingHNSWMaxLoadedIndexes, "global"),
		}
	}

	// Create vector store.
	// Priority: Qdrant (if configured) > PgVector (if Postgres) > HNSWStore (if SQLite).
	var vectorStore storage.VectorStore
	var hnswStore *storage.HNSWStore
	var qdrantStore *storage.QdrantStore
	// Faceted SQL backends size their Search over-fetch to the configured
	// max_facets; HNSW needs no resolver (its graph yields one node per memory
	// and it brute-forces every topic facet). Shared so the two wirings cannot
	// drift.
	maxFacetsResolver := func() int {
		return settingsSvc.ResolveIntWithDefault(context.Background(), service.SettingMultiVectorMaxFacets, "global")
	}
	// The faceted Search path is gated on the multi-vector feature switch and a
	// per-namespace topic-facet presence probe (cached for the configured TTL),
	// so recall skips its facet work when the feature is off or a namespace has
	// no topic facets. Shared across all three backends so the wirings can't drift.
	facetsEnabledResolver := func() bool {
		return settingsSvc.ResolveBoolWithDefault(context.Background(), service.SettingMultiVectorEnabled, "global")
	}
	facetPresenceTTLResolver := func() time.Duration {
		return settingsSvc.ResolveDurationSecondsWithDefault(context.Background(), service.SettingMultiVectorFacetPresenceCacheTTL, "global")
	}
	if qdrantCfg.Addr != "" {
		// Only adopt Qdrant on a successful construction; assigning the
		// (nil, err) return straight into the interface would leave a non-nil
		// interface wrapping a typed-nil store and suppress the pgvector/HNSW
		// fallback below.
		qs, qerr := storage.NewQdrantStore(qdrantCfg)
		if qerr != nil {
			slog.Warn("boot: qdrant connection failed, vector search disabled", "err", qerr)
		} else {
			vectorStore = qs
			qdrantStore = qs
			qs.SetMaxFacetsResolver(maxFacetsResolver)
			qs.SetFacetGate(facetsEnabledResolver, facetPresenceTTLResolver)
		}
	}
	if vectorStore == nil && db.Backend() == storage.BackendPostgres && cfg.Database.URL != "" {
		pgvStore, pgvErr := storage.NewPgVectorStore(cfg.Database.URL)
		if pgvErr != nil {
			slog.Warn("boot: pgvector connection failed, vector search disabled", "err", pgvErr)
		} else {
			vectorStore = pgvStore
			pgvStore.SetMaxFacetsResolver(maxFacetsResolver)
			pgvStore.SetFacetGate(facetsEnabledResolver, facetPresenceTTLResolver)
			slog.Info("boot: pgvector store initialized")
		}
	}
	if vectorStore == nil && db.Backend() == storage.BackendSQLite {
		hnswCfg := buildHNSWConfig()
		hnswStore = storage.NewHNSWStore(db.DB(), db.WriteDB(), hnswCfg)
		hnswStore.SetFacetGate(facetsEnabledResolver, facetPresenceTTLResolver)
		vectorStore = hnswStore
		defer func() { _ = hnswStore.Close() }()
		slog.Info("boot: hnsw vector store initialized (SQLite backend)",
			"m", hnswCfg.M, "ef_construction", hnswCfg.EfConstruction,
			"ef_search", hnswCfg.EfSearch, "max_loaded", hnswCfg.MaxLoadedIndexes)
	}

	// Activation guard: if Qdrant is the active store but holds no memory
	// vectors while the SQL store still does, the operator most likely set
	// qdrant.addr and restarted without migrating. Recall is degraded until a
	// migration runs, so warn loudly rather than fail silently.
	if qdrantStore != nil {
		// Check the SQL side first (one cheap query): only probe Qdrant when
		// there are vectors that would actually need migrating, so a fresh
		// deployment skips the per-dimension Qdrant round trips at startup.
		if sqlCount := storage.MemoryVectorCount(bootCtx, db); sqlCount > 0 {
			if qCount, err := qdrantStore.TotalMemoryVectors(bootCtx); err == nil && qCount == 0 {
				slog.Warn("boot: Qdrant is the active vector store but its memory collections are empty while the SQL store holds vectors; recall will be degraded until you migrate (Admin -> Settings -> Vector Database -> Migrate)", "sql_memory_vectors", sqlCount)
			}
		}
	}

	// Wrap the vector store with metrics instrumentation so every Search
	// call lands in nram_vector_search_duration_seconds. The wrapper is a
	// no-op when promMetrics is nil. Wrap before entityRepo.SetVectorStore
	// so the entity repo's promoteStub path is instrumented too.
	vectorStore = metrics.WrapVectorStore(vectorStore, promMetrics)

	// Wire the vector store into the entity repo so promoteStub (called from
	// EntityRepo.Upsert when a real-typed entity is upserted over an existing
	// stub) can opportunistically clean up the stub's vector. SQL-backed
	// stores cascade via entity_vectors_*; this is materially load-bearing
	// only for Qdrant, which has no SQL FK to entities.
	if vectorStore != nil {
		entityRepo.SetVectorStore(vectorStore)
	}

	// Create event bus. Buffer and replay capacity are read once from
	// settings; runtime changes require server restart.
	eventBusBuf := settingsSvc.ResolveIntWithDefault(context.Background(),
		service.SettingEventsSubscriberBufferSize, "global")
	eventBusReplay := settingsSvc.ResolveIntWithDefault(context.Background(),
		service.SettingEventsReplayCapacity, "global")
	eventBus := events.NewEventBus(db.Backend(), nil, eventBusBuf, eventBusReplay)
	defer func() { _ = eventBus.Close() }()

	// Create webhook deliverer.
	webhookDeliverer := events.NewWebhookDeliverer(eventBus, webhookRepo)
	delivererCtx, delivererCancel := context.WithCancel(context.Background())
	defer delivererCancel()
	go func() {
		if err := webhookDeliverer.Start(delivererCtx); err != nil {
			slog.Warn("boot: webhook deliverer stopped", "err", err)
		}
	}()

	// Create services.
	storeSvc := service.NewStoreService(
		memoryRepo, projectRepo, namespaceRepo,
		ingestionLogRepo, enrichmentQueueRepo,
		settingsSvc,
	).WithMetrics(promMetrics)
	recallSvc := service.NewRecallService(
		memoryRepo, projectRepo, namespaceRepo,
		vectorStore, entityRepo,
		relationshipRepo, embedProvider,
	).WithMetrics(promMetrics)
	// graphReaper removes knowledge-graph data whose sourcing memory is gone
	// (hard-deleted, soft-deleted, or superseded) and keeps entity
	// mention_count consistent with surviving provenance. Shared by the
	// forget, update (supersede), and lifecycle services.
	graphReaper := service.NewGraphReaper(relationshipRepo, entityRepo)
	updateSvc := service.NewUpdateService(
		memoryRepo, projectRepo,
		vectorStore, embedProvider, enrichmentQueueRepo,
	).WithGraphReaper(graphReaper)
	forgetSvc := service.NewForgetService(
		memoryRepo, projectRepo, vectorStore,
		lineageRepo,
	).WithMetrics(promMetrics).WithGraphReaper(graphReaper)
	moveSvc := service.NewMoveService(memoryRepo, projectRepo, storeSvc, forgetSvc)
	batchGetSvc := service.NewBatchGetService(memoryRepo, projectRepo)
	batchStoreSvc := service.NewBatchStoreService(
		memoryRepo, projectRepo, namespaceRepo,
		ingestionLogRepo, enrichmentQueueRepo,
		settingsSvc,
	).WithMetrics(promMetrics)
	var hnswDeleter service.HNSWSnapshotDeleter
	if hnswStore != nil {
		hnswDeleter = hnswStore
	}
	projectDeleteSvc := service.NewProjectDeleteService(
		db,
		projectRepo, projectRepo, memoryRepo, lineageRepo, memoryRepo,
		vectorStore, entityAliasRepo, entityRepo, relationshipRepo,
		enrichmentQueueRepo, tokenUsageRepo,
		ingestionLogRepo, hnswDeleter, namespaceRepo, eventBus,
	)
	enrichSvc := service.NewEnrichService(memoryRepo, projectRepo, enrichmentQueueRepo, lineageRepo)
	enrichSvc.AttachAugmentationLister(memoryRepo)
	enrichSvc.AttachMultiVectorLister(memoryRepo)
	enrichSvc.AttachMissingEmbeddingLister(memoryRepo)
	enrichSvc.AttachDreamEntityLister(memoryRepo)
	// Procedural tier: verbatim standing instructions. Holds no enrichment,
	// embedder, or dream dependency by design; that absence keeps it verbatim.
	proceduralSvc := service.NewProceduralService(proceduralRepo)
	enrichSvc.AttachParaphraseCandidateLister(memoryRepo)
	enrichSvc.AttachReExtract(memoryRepo, graphReaper)
	exportSvc := service.NewExportService(
		memoryRepo, entityRepo, relationshipRepo, lineageRepo, projectRepo,
		settingsSvc,
	)
	importSvc := service.NewImportService(
		memoryRepo, projectRepo, namespaceRepo, ingestionLogRepo,
		entityRepo, relationshipRepo, lineageRepo,
		settingsSvc,
	)

	// Self-service export job worker. Replaces the truncation-bound MCP
	// export tool: large multi-project exports run asynchronously, land
	// on disk under SettingExportArtifactDir (default <cwd>/exports), and
	// the caller downloads them through /v1/me/exports/{job_id}/download.
	// workerID is a per-process UUID so claim-loss detection can
	// distinguish workers across instances; dataDir is "." so SQLite
	// deployments get the artifact tree next to the database file.
	exportJobWorkerID := "export-worker-" + uuid.NewString()
	exportJobSvc := service.NewExportJobService(
		exportJobRepo, userRepo, projectRepo, exportSvc, settingsSvc,
		exportJobWorkerID, ".", nil,
	)
	exportJobCtx, exportJobCancel := context.WithCancel(context.Background())
	defer exportJobCancel()
	go exportJobSvc.Run(exportJobCtx)

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
	// column on Postgres). FusionConfig and ranking weights are resolved
	// live per recall via the wired SettingsService below, so admin-UI
	// edits to recall.fusion.* and ranking.weight.* apply on the next call
	// without a server restart.
	recallSvc.SetLexical(memoryRepo)
	recallSvc.SetSettings(settingsSvc)
	recallSvc.SetVectorHydrator(vectorStore)

	// Reranker slot, read live so a hot provider reload is picked up without a
	// restart. The slot has no fallback, so an unconfigured slot yields nil and
	// the recall/ask rerank stages stay inert. The stages are additionally gated
	// off by default (ranking.rerank.enabled / ask.rerank.enabled), so wiring the
	// accessor here changes nothing until an operator configures the slot and
	// flips the toggle.
	rerankProvider := func() provider.RerankProvider {
		if registry == nil {
			return nil
		}
		return registry.GetReranker()
	}
	recallSvc.SetReranker(rerankProvider)

	// One-shot startup audit of the recall-tuning surface. The values
	// themselves are resolved per recall (see RecallService.resolveFusion);
	// these logs surface only the deployment-time concerns: stored values
	// drifting from registered defaults after an upgrade, and an outright
	// misconfigured fusion that would silently produce zero candidates.
	logFusionStartupAudit(context.Background(), settingsSvc)

	// Create lifecycle service for TTL expiry and purge sweeps. All
	// lifecycle.* knobs (sweep interval, batch size, orphan-grace cutoff,
	// purge delay) are resolved live from the settings registry per sweep
	// iteration, so operators can tune them from the admin UI without
	// restarting.
	graphPruner := service.NewGraphPruner(entityRepo, relationshipRepo)
	lifecycleSvc := service.NewLifecycleService(memoryRepo, vectorStore, graphPruner, service.LifecycleConfig{}, settingsSvc).
		WithGraphReaper(graphReaper)
	lifecycleSvc.Start()
	defer lifecycleSvc.Stop()

	// Read live so a hot provider reload reopens or closes the gate
	// without a restart.
	enrichmentAvailable := func() bool {
		return registry != nil && registry.EnrichmentAvailable()
	}

	// Ask synthesis tool. Reads the dedicated ask provider slot live (so an
	// admin provider edit is picked up without restart); the slot has no
	// fallback, so an unconfigured slot yields nil and the service returns a
	// clear "synthesis provider not configured" error rather than borrowing the
	// enrichment provider. The feature flag (ask.enabled) gates visibility at
	// the MCP filter and REST AskGate, not here.
	askProvider := func() provider.LLMProvider {
		if registry == nil {
			return nil
		}
		return registry.GetAsk()
	}
	askSvc := service.NewAskService(
		recallSvc, memoryRepo, projectRepo, relationshipRepo, askProvider, settingsSvc,
	).WithMetrics(promMetrics).WithVectorHydrator(vectorStore).WithReranker(rerankProvider)

	// Create MCP server.
	mcpServer := mcp.NewServer(mcp.Dependencies{
		Backend:        db.Backend(),
		InstanceID:     instanceIdentity.ID.String(),
		Store:          storeSvc,
		Recall:         recallSvc,
		Ask:            askSvc,
		Forget:         forgetSvc,
		Update:         updateSvc,
		BatchGet:       batchGetSvc,
		BatchStore:     batchStoreSvc,
		ProjectDelete:  projectDeleteSvc,
		ProjectUpdater: projectRepo,
		ProjectRepo:    projectRepo,
		Procedural:     proceduralSvc,
		UserRepo:       userRepo,
		NamespaceRepo:  namespaceRepo,
		MemoryLister:   memoryRepo,
		EntityReader:   entityRepo,
		Traverser:      relationshipRepo,
		Settings:       settingsSvc,
		EventBus:       eventBus,
		Metrics:        promMetrics,
		ProviderStatus: func() (bool, bool) {
			if registry == nil {
				return false, false
			}
			hasEmbed := registry.GetEmbedding() != nil
			hasEnrich := registry.GetFact() != nil && registry.GetEntity() != nil
			return hasEmbed, hasEnrich
		},
	})

	// Build start time for health handler.
	startTime := time.Now()

	// Create setup checker (cached atomic bool, queries DB once).
	setupChecker := api.NewSetupChecker(db)

	// Create admin store adapters.
	setupStore := adminstore.NewSetupStore(userRepo, namespaceRepo, orgRepo, apiKeyRepo, projectRepo, db)

	// Headless administrator bootstrap. When admin.email and admin.password
	// are both supplied via config.yaml or NRAM_ADMIN_EMAIL/NRAM_ADMIN_PASS,
	// the first administrator is created automatically, bypassing the setup
	// wizard. After setup is complete the call is a no-op so re-running with
	// the same env vars is safe across restarts.
	if runHeadlessBootstrap(context.Background(), setupStore, cfg.Admin) {
		setupChecker.MarkComplete()
	}

	orgAdminStore := adminstore.NewOrgAdminStore(orgRepo, namespaceRepo)
	userAdminStore := adminstore.NewUserAdminStore(userRepo, apiKeyRepo, namespaceRepo, orgRepo, projectRepo)
	webhookAdminStore := adminstore.NewWebhookAdminStore(webhookRepo)
	settingsAdminStore := adminstore.NewSettingsAdminStore(settingsRepo, settingsSvc)
	dashboardStore := adminstore.NewDashboardStore(db, enrichmentQueueRepo)
	analyticsStore := adminstore.NewAnalyticsStore(db)
	usageStore := adminstore.NewUsageStore(db)
	aggregatesStore := adminstore.NewAggregatesStore(db)
	auditStore := adminstore.NewAuditStore(db)
	databaseAdminStore := adminstore.NewDatabaseAdminStore(db, eventBus)
	vectorMigrationStore := adminstore.NewVectorMigrationAdminStore(db, cfg.Database.URL, buildHNSWConfig(), func(ctx context.Context) storage.QdrantConfig {
		return storage.QdrantConfig{
			Addr:             service.ResolveOrDefault(ctx, settingsSvc, service.SettingQdrantAddr, "global"),
			APIKey:           service.ResolveOrDefault(ctx, settingsSvc, service.SettingQdrantAPIKey, "global"),
			UseTLS:           settingsSvc.ResolveBool(ctx, service.SettingQdrantUseTLS, "global"),
			PoolSize:         uint(settingsSvc.ResolveIntWithDefault(ctx, service.SettingQdrantPoolSize, "global")),
			KeepAliveTime:    settingsSvc.ResolveIntWithDefault(ctx, service.SettingQdrantKeepAliveTime, "global"),
			KeepAliveTimeout: uint(settingsSvc.ResolveIntWithDefault(ctx, service.SettingQdrantKeepAliveTimeout, "global")),
		}
	}, eventBus)
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

	// Live provider accessors for the enrichment worker and test-prompt
	// surface. Each reads the registry on every call so an admin provider
	// edit (registry.Reload) is picked up without restart. The query-augment
	// and ingestion-decision accessors return their dedicated slot when
	// configured, else fall back to the fact provider, so the worker phases
	// get a working provider either way.
	providerFactory := func(get func(*provider.Registry) provider.LLMProvider) func() provider.LLMProvider {
		return func() provider.LLMProvider {
			if registry == nil {
				return nil
			}
			return get(registry)
		}
	}
	factProvider := providerFactory((*provider.Registry).GetFact)
	entityProvider := providerFactory((*provider.Registry).GetEntity)
	queryAugmentProvider := providerFactory((*provider.Registry).GetQueryAugment)
	ingestionProvider := providerFactory((*provider.Registry).GetIngestionDecision)

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

	// Start enrichment worker pool: needs providers for LLM extraction.
	workerPool := enrichment.NewWorkerPool(
		enrichment.WorkerConfig{Backend: db.Backend()},
		memoryRepo, memoryRepo, memoryRepo, memoryRepo, enrichmentQueueRepo,
		entityRepo, relationshipRepo, lineageRepo, vectorStore,
		factProvider, entityProvider, embedProvider,
		ingestionProvider, queryAugmentProvider, ingestionDedup, settingsSvc, cascadeResolver,
		eventBus,
	).WithMetrics(promMetrics)
	workerPool.Start()
	defer workerPool.Stop()
	slog.Info("boot: enrichment worker pool started")

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
	slog.Info("boot: enrichment stuck-job sweeper started")

	// Create dreaming system.
	dreamCycleRepo := storage.NewDreamCycleRepo(db)
	dreamLogRepo := storage.NewDreamLogRepo(db)
	dreamDirtyRepo := storage.NewDreamDirtyRepo(db)

	consolidationPhase := dreaming.NewConsolidationPhase(memoryRepo, memoryRepo, lineageRepo, factProvider, embedProvider, settingsSvc, enrichmentQueueRepo)
	contradictionPhase := dreaming.NewContradictionPhase(memoryRepo, memoryRepo, lineageRepo, factProvider, embedProvider, settingsSvc)
	projectDescriptionPhase := dreaming.NewProjectDescriptionPhase(projectRepo, memoryRepo, memoryRepo, enrichmentQueueRepo, settingsSvc)
	// Wire the active vector store into dream-side state transitions so that
	// demotion and supersession purge vectors alongside the row-level update,
	// and so the contradiction phase reads stored vectors instead of
	// re-embedding the namespace every cycle.
	if vectorStore != nil {
		consolidationPhase.AttachVectorPurger(vectorStore)
		consolidationPhase.AttachVectorStore(vectorStore)
		contradictionPhase.AttachVectorStore(vectorStore)
		contradictionPhase.AttachVectorPurger(vectorStore)
		projectDescriptionPhase.AttachVectorPurger(vectorStore)
		memoryRepo.AttachVectorStore(vectorStore)
	}

	heartbeatInterval := settingsSvc.ResolveDurationSecondsWithDefault(
		context.Background(), service.SettingDreamHeartbeatInterval, "global")

	dreamRunner := dreaming.NewRunner(
		dreamCycleRepo, dreamLogRepo, workerPool, heartbeatInterval, eventBus, settingsSvc,
		// Reconciles each project's description into a single embedded backing
		// memory. Runs first so the freshly synced, marked row exists before the
		// mutating phases (paraphrase/contradiction/consolidation/pruning), which
		// the shield keeps off project-description rows anyway. SQL-only (frac 0).
		projectDescriptionPhase,
		dreaming.NewEntityDedupPhase(entityRepo, entityRepo, entityAliasRepo, relationshipRepo, relationshipRepo, vectorStore, settingsSvc),
		// Embedding backfill repairs two divergences: rows whose embedding_dim is
		// set but whose memory_vectors_<dim> row is missing (no_vector), and rows
		// whose embedding_dim is NULL (restamp when the vector survived, re-embed
		// otherwise). Runs before paraphrase dedup so the downstream phase sees the
		// repaired vector state, and before multi-vector backfill so restored
		// embedding_dims become facet candidates in the same cycle.
		dreaming.NewEmbeddingBackfillPhase(memoryRepo, memoryRepo, vectorStore, embedProvider, settingsSvc),
		// Uncovered backfill re-enqueues a FULL enrichment job for every live
		// memory that holds no enrichment job at all (enrichment disabled at
		// creation, a failed write-time enqueue, a direct import/migration) — the
		// unconditional safety net replacing the removed
		// NRAM_ENABLE_ENRICHMENT_BACKFILL boot hook, gated only on enrichment.enabled.
		// Unlike embedding backfill (which only re-embeds), the full job restores
		// extracted facts, entities, and relationships. Runs before the narrower
		// backfill phases; a memory it enqueues is skipped by them via the partial
		// unique index idx_enrichment_queue_pending_memory. Enqueue-only, no LLM, so
		// it consumes no dream token budget and is deliberately absent from runner's
		// phaseFractionKeys (like the SQL-only projectDescriptionPhase; the other
		// backfill siblings register a 0.0 fraction instead, which resolves the same).
		dreaming.NewUncoveredBackfillPhase(storage.NewUncoveredBackfiller(db), settingsSvc),
		// Augmentation backfill enqueues query-augmentation jobs for rows whose
		// vector was built from raw content (augmented_embedding_at IS NULL),
		// automating recovery for memories whose augmentation step was skipped.
		// Runs before consolidation so it only sweeps rows stranded by prior
		// cycles, not the fresh syntheses consolidation enqueues itself.
		dreaming.NewAugmentationBackfillPhase(memoryRepo, enrichmentQueueRepo, settingsSvc),
		// Multi-vector backfill enqueues facet-only jobs for vectored memories not
		// yet faceted (faceted_at IS NULL), automating per-topic faceting so it
		// self-drains each cycle. Runs after embedding backfill so embedding_dims
		// restored this cycle are already visible as facet candidates.
		dreaming.NewMultiVectorBackfillPhase(memoryRepo, enrichmentQueueRepo, settingsSvc),
		// Consolidation entity backfill enqueues entity-only jobs for active
		// consolidation dreams that still lack any sourced relationship, recovering
		// entity-graph coverage stranded before dreams were extracted. Runs before
		// consolidation (like the augmentation backfill) so it sweeps prior-cycle
		// dreams, not the fresh syntheses this cycle's consolidation enqueues, which
		// the worker covers via their own enrichment job.
		dreaming.NewConsolidationEntityBackfillPhase(memoryRepo, enrichmentQueueRepo, settingsSvc),
		// Paraphrase dedup runs before contradiction so the LLM-judge pair
		// walk operates on a deduped memory set.
		dreaming.NewParaphraseDedupPhase(memoryRepo, memoryRepo, vectorStore, vectorStore, embedProvider, settingsSvc),
		dreaming.NewTransitivePhase(entityRepo, relationshipRepo, relationshipRepo, settingsSvc),
		contradictionPhase,
		consolidationPhase,
		dreaming.NewPruningPhase(memoryRepo, memoryRepo, relationshipRepo, relationshipRepo, settingsSvc),
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
		slog.Warn("boot: dream dirty tracker failed to start", "err", err)
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
	slog.Info("boot: dream scheduler started")

	// Start the stuck-cycle sweeper in its own goroutine. Lifecycle is
	// independent of the scheduler so a long-running cycle on this instance
	// (which blocks the scheduler's main loop) can't also block the sweeper
	// that's supposed to detect and recover from it.
	dreamStuckSweeper := dreaming.NewStuckCycleSweeper(
		dreamCycleRepo, dreamScheduler, settingsSvc, eventBus,
	)
	dreamStuckSweeper.Start()
	defer dreamStuckSweeper.Stop()
	slog.Info("boot: dream stuck-cycle sweeper started")

	dreamAdminStore := adminstore.NewDreamAdminStore(
		dreamCycleRepo, dreamLogRepo, dreamDirtyRepo, settingsRepo,
		settingsSvc, dreamScheduler, projectRepo, cascadeResolver, db,
	)

	// Create auth config for login/lookup handlers.
	// JWT secret is loaded later, but we need it here; load it early.
	jwtSecret, err := storage.LoadOrCreateJWTSecret(context.Background(), db)
	if err != nil {
		log.Fatalf("failed to load jwt secret: %v", err)
	}

	// Share-token repo + service back the capability-bearer flow that lets
	// owners delegate scoped access to external recipients without an nram
	// account. The service brokers the oauth_repo cascade-revoke on share
	// revoke and is also passed to the auth middleware (resolves bearer-
	// direct nram_s_* tokens and hydrates JWT-carried share_token_id claims).
	shareTokenRepo := storage.NewShareTokenRepo(db)
	shareTokenSvc := service.NewShareTokenService(shareTokenRepo, oauthRepo)

	// Wire the share-token sweep onto the project-delete cascade. When a
	// project deletion drops a share's last grant via the FK cascade, the
	// sweep post-commit auto-revokes the share so it does not linger as
	// "active" in the owner's UI.
	projectDeleteSvc.WithShareSweeper(shareTokenSvc, userRepo)

	// Create OAuth server. Base URL for metadata, JWT audience, etc. is derived
	// from the request Host header automatically, no configuration needed.
	// Share-paste consent + magic-link landing depend on the share-token
	// service and a project lookup for the grants display.
	oauthServer := auth.NewOAuthServer(oauthRepo, userRepo, jwtSecret).
		WithShareTokens(shareTokenSvc, projectRepo).
		WithInstanceID(instanceIdentity.ID.String())

	// Session-JWT TTL and refresh threshold are runtime-configurable via
	// the settings registry (auth.session_token_ttl_seconds /
	// auth.session_refresh_threshold_seconds). Single shared instance:
	// every issuance and refresh path resolves through the same cache.
	sessionTimings := service.NewSettingsBackedSessionTimings(settingsSvc)

	authCfg := api.AuthConfig{
		UserRepo:    userRepo,
		IdPRepo:     oauthRepo,
		PasskeyRepo: webauthnRepo,
		JWTSecret:   jwtSecret,
		Timings:     sessionTimings,
	}

	// Create WebAuthn handler for passkey registration and login.
	webauthnHandler := auth.NewWebAuthnHandler(auth.WebAuthnHandlerConfig{
		CredRepo:  webauthnRepo,
		UserRepo:  userRepo,
		JWTSecret: jwtSecret,
		Timings:   sessionTimings,
	})
	defer webauthnHandler.Close()

	// Create IdP SSO handler for external identity provider flows.
	idpHandler := auth.NewIdPHandler(auth.IdPHandlerConfig{
		IdPRepo:    oauthRepo,
		UserRepo:   userRepo,
		UserCreate: userAdminStore,
		JWTSecret:  jwtSecret,
		Timings:    sessionTimings,
	})

	// Build the /v1/me/exports handler trio once: one factory, three named
	// fields the server.Handlers struct points to.
	meExportHandlers := api.NewMeExportHandlers(exportJobSvc)

	// Project access config, used both by the move handlers (to authorize the
	// destination project supplied in the request body) and by the route-level
	// ProjectAccessMiddleware below.
	projectAccessCfg := api.ProjectAccessConfig{
		Projects:   projectRepo,
		Namespaces: namespaceRepo,
		Orgs:       orgRepo,
		Users:      userRepo,
	}

	// Per-memory re-extract (queue UI per-row action), shared across the
	// admin/org/self enrichment surfaces. namespacePrefix is the path-prefix
	// scope ("" = global, the admin path); the tier handlers supply the caller's
	// scope so out-of-scope IDs are silently dropped.
	reExtractMemoriesFn := func(ctx context.Context, namespacePrefix string, memoryIDs []uuid.UUID) (api.EnrichmentReExtractResult, error) {
		resp, err := enrichSvc.ReExtract(ctx, &service.ReExtractRequest{
			NamespacePrefix: namespacePrefix,
			MemoryIDs:       memoryIDs,
		})
		if err != nil {
			return api.EnrichmentReExtractResult{}, err
		}
		return api.EnrichmentReExtractResult{
			CandidateCount:      resp.CandidateCount,
			Enqueued:            resp.Enqueued,
			EntitiesRecomputed:  resp.EntitiesRecomputed,
			FactChildrenRemoved: resp.FactChildrenRemoved,
		}, nil
	}

	// Assemble handlers.
	handlers := server.Handlers{
		// Health
		Health: api.NewHealthHandler(api.HealthConfig{
			DB:        db,
			Providers: registry,
			Queue:     enrichmentQueueRepo,
			Version:   version.Version,
			Build:     version.Get(),
			StartTime: startTime,
		}),

		// OpenAPI spec served at GET /openapi.yaml
		OpenAPISpec: api.NewOpenAPIHandler(),

		// Agent instructions/rules served as plain text at GET /instructions
		Instructions: api.NewInstructionsHandler(),

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
		Ask:        api.NewAskHandler(askSvc, userRepo, projectRepo),
		BulkForget: api.NewBulkForgetHandler(forgetSvc, eventBus),
		Move:       api.NewMoveHandler(moveSvc, projectAccessCfg, eventBus),
		BulkMove:   api.NewBulkMoveHandler(moveSvc, projectAccessCfg, eventBus),
		Enrich:     api.NewEnrichHandler(enrichSvc, eventBus),
		Export:     api.NewExportHandler(exportSvc),
		Import:     api.NewImportHandler(importSvc),
		PreviewAugment: api.NewMemoryPreviewAugmentHandler(api.MemoryPreviewAugmentConfig{
			Memories:             memoryRepo,
			Projects:             projectRepo,
			QueryAugmentProvider: queryAugmentProvider,
			Settings:             settingsSvc,
		}),

		// User-scoped handlers
		MeRecall:            api.NewMeRecallHandler(recallSvc, userRepo),
		MeAsk:               api.NewMeAskHandler(askSvc, userRepo),
		MeProjects:          api.NewMeProjectsHandler(projectRepo, userRepo, namespaceRepo, eventBus),
		MeProjectItem:       api.NewMeProjectItemHandler(projectRepo, userRepo, eventBus),
		MeProjectDelete:     api.NewMeProjectDeleteHandler(projectDeleteSvc, projectRepo, userRepo),
		MeProcedural:        api.NewMeProceduralHandler(proceduralSvc, userRepo),
		MeProceduralItem:    api.NewMeProceduralItemHandler(proceduralSvc, userRepo),
		MeProceduralDelete:  api.NewMeProceduralDeleteHandler(proceduralSvc, userRepo),
		MeProceduralExport:  api.NewMeProceduralExportHandler(proceduralSvc, userRepo),
		MeProceduralImport:  api.NewMeProceduralImportHandler(proceduralSvc, userRepo),
		MeAPIKeys:           api.NewMeAPIKeysHandler(apiKeyRepo, auditStore),
		MeAPIKeyRevoke:      api.NewMeAPIKeyRevokeHandler(apiKeyRepo, auditStore),
		MeOAuthClients:      api.NewMeOAuthClientsHandler(oauthRepo),
		MeOAuthClientRevoke: api.NewMeOAuthClientRevokeHandler(oauthRepo, auditStore),
		MeShares:            api.NewMeSharesHandler(shareTokenSvc, projectRepo, userRepo),
		MeShareItem:         api.NewMeShareItemHandler(shareTokenSvc, projectRepo, userRepo),
		MeChangePassword:    api.NewMeChangePasswordHandler(userRepo, auditStore),
		MeProfile:           api.NewMeProfileHandler(userRepo),
		MeProfilePatch:      api.NewMeProfilePatchHandler(userRepo),

		// Self-tier system-pipeline observability: read-only views of the
		// caller's own dream cycles + enrichment queue items. Write
		// operations (enable/abandon/rollback for dreaming, retry/pause
		// for enrichment) remain admin-only at /v1/admin/*.
		MeDreaming: api.NewSelfDreamingHandler(api.MeDreamingConfig{
			Store:      dreamAdminStore,
			Projects:   projectRepo,
			Namespaces: namespaceRepo,
			Users:      userRepo,
			Gate:       dreamAdminStore,
			Rollback:   dreamRollback,
		}),
		MeEnrichment: api.NewSelfEnrichmentHandler(api.MeEnrichmentConfig{
			Store:             enrichmentAdminStore,
			Users:             userRepo,
			Namespaces:        namespaceRepo,
			ReExtractMemories: reExtractMemoriesFn,
		}),
		MeCapabilities: api.NewMeCapabilitiesHandler(api.MeCapabilitiesConfig{
			EnrichmentAvailable: enrichmentAvailable,
			Settings:            settingsSvc,
		}),
		MeRankingWeightsDefaults: api.NewMeRankingWeightsDefaultsHandler(api.MeRankingWeightsDefaultsConfig{
			Store: settingsAdminStore,
		}),
		MeSettingDefaults: api.NewMeSettingDefaultsHandler(api.MeSettingDefaultsConfig{
			Store: settingsAdminStore,
		}),

		// Self-service exports: caller-only. No admin equivalent (the
		// codebase's privacy invariant test in internal/server keeps
		// memory content off admin surfaces). Authentication is enforced
		// by the parent /v1/me group; per-row ownership is enforced
		// inside the service against (job.user_id == caller.user_id).
		MeExports:        meExportHandlers.List,
		MeExportItem:     meExportHandlers.Item,
		MeExportDownload: meExportHandlers.Download,

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

		// Standalone public API reference page.
		Docs: ui.DocsHandler(),

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
		OAuthAuthorizeContext:  oauthServer.AuthorizeContextHandler(),
		OAuthSharePreview:      oauthServer.SharePreviewHandler(),
		OAuthAuthorizeComplete: oauthServer.AuthorizeCompleteHandler(),
		ShareAccept:            oauthServer.ShareAcceptHandler(),

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
			Timings:    sessionTimings,
			OnComplete: setupChecker.MarkComplete,
			Audit:      auditStore,
		}),
		AdminOnboarding:    api.NewAdminOnboardingHandler(api.SetupConfig{Store: setupStore}),
		AdminDashboard:     api.NewAdminDashboardHandler(api.DashboardConfig{Store: dashboardStore}),
		AdminActivity:      api.NewAdminActivityHandler(api.DashboardConfig{Store: dashboardStore}),
		AdminOrgs:          api.NewAdminOrgsHandler(api.OrgAdminConfig{Store: orgAdminStore, Audit: auditStore}),
		AdminUsers:         api.NewAdminUsersHandler(api.UserAdminConfig{Store: userAdminStore, Audit: auditStore}),
		AdminProviders:     api.NewAdminProvidersHandler(api.ProviderAdminConfig{Store: providerAdminStore}),
		AdminSettings:      api.NewAdminSettingsHandler(api.SettingsAdminConfig{Store: settingsAdminStore}),
		AdminSettingsReset: api.NewAdminSettingsResetHandler(api.SettingsAdminConfig{Store: settingsAdminStore}),
		AdminEnrichment: api.NewAdminEnrichmentHandler(api.EnrichmentAdminConfig{
			Store:          enrichmentAdminStore,
			FactProvider:   factProvider,
			EntityProvider: entityProvider,
			FactSystemPromptDefault: func(ctx context.Context) string {
				return service.ResolveOrDefault(ctx, settingsSvc, service.SettingFactSystemPrompt, "global")
			},
			EntitySystemPromptDefault: func(ctx context.Context) string {
				return service.ResolveOrDefault(ctx, settingsSvc, service.SettingEntitySystemPrompt, "global")
			},
			RelationshipSystemPromptDefault: func(ctx context.Context) string {
				return service.ResolveOrDefault(ctx, settingsSvc, service.SettingRelationshipSystemPrompt, "global")
			},
			QueryAugmentSystemPromptDef: func(ctx context.Context) string {
				return service.ResolveOrDefault(ctx, settingsSvc, service.SettingQueryAugmentSystemPrompt, "global")
			},
			IngestionSystemPromptDefault: func(ctx context.Context) string {
				return service.ResolveOrDefault(ctx, settingsSvc, service.SettingIngestionDecisionSystemPrompt, "global")
			},
			TestPromptMaxTokens: func(ctx context.Context) int {
				return settingsSvc.ResolveIntWithDefault(ctx, service.SettingEnrichmentTestPromptMaxTokens, "global")
			},
			// The augment/ingestion test surface runs against the dedicated
			// provider slots (falling back to fact when unconfigured), matching
			// the runtime phases.
			QueryAugmentProvider: queryAugmentProvider,
			IngestionProvider:    ingestionProvider,
			BackfillAugmentation: func(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int) (int, int, error) {
				resp, err := enrichSvc.BackfillAugmentation(ctx, &service.BackfillAugmentationRequest{
					ProjectID: projectID,
					DryRun:    dryRun,
					Limit:     limit,
				})
				if err != nil {
					return 0, 0, err
				}
				return resp.CandidateCount, resp.Enqueued, nil
			},
			BackfillExtractedFactParaphrase: func(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int) (int, int, error) {
				resp, err := enrichSvc.BackfillExtractedFactParaphrase(ctx, &service.BackfillExtractedFactParaphraseRequest{
					ProjectID: projectID,
					DryRun:    dryRun,
					Limit:     limit,
				})
				if err != nil {
					return 0, 0, err
				}
				return resp.CandidateCount, resp.Enqueued, nil
			},
			BackfillMultiVector: func(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int) (int, int, error) {
				resp, err := enrichSvc.BackfillMultiVector(ctx, &service.BackfillMultiVectorRequest{
					ProjectID: projectID,
					DryRun:    dryRun,
					Limit:     limit,
				})
				if err != nil {
					return 0, 0, err
				}
				return resp.CandidateCount, resp.Enqueued, nil
			},
			RelabelGraph: func(ctx context.Context, dryRun bool) (api.EnrichmentRelabelResult, error) {
				retyped, merged, err := entityRepo.RelabelEntities(ctx, dryRun)
				if err != nil {
					return api.EnrichmentRelabelResult{}, err
				}
				rows, before, after, err := relationshipRepo.RelabelRelations(ctx, dryRun)
				if err != nil {
					return api.EnrichmentRelabelResult{}, err
				}
				return api.EnrichmentRelabelResult{
					EntitiesRetyped:         retyped,
					EntitiesMerged:          merged,
					RelationRowsRelabeled:   rows,
					DistinctRelationsBefore: before,
					DistinctRelationsAfter:  after,
				}, nil
			},
			BackfillEmbeddingDims: func(ctx context.Context) (int64, error) {
				return entityRepo.BackfillEmbeddingDimFromVectors(ctx)
			},
			ReExtract: func(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int) (api.EnrichmentReExtractResult, error) {
				resp, err := enrichSvc.ReExtract(ctx, &service.ReExtractRequest{
					ProjectID: projectID,
					DryRun:    dryRun,
					Limit:     limit,
				})
				if err != nil {
					return api.EnrichmentReExtractResult{}, err
				}
				return api.EnrichmentReExtractResult{
					CandidateCount:      resp.CandidateCount,
					Enqueued:            resp.Enqueued,
					EntitiesRecomputed:  resp.EntitiesRecomputed,
					FactChildrenRemoved: resp.FactChildrenRemoved,
				}, nil
			},
			ReExtractMemories: reExtractMemoriesFn,
			BackfillMissingEmbeddings: func(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int) (int, int, error) {
				resp, err := enrichSvc.BackfillMissingEmbeddings(ctx, &service.BackfillMissingEmbeddingsRequest{
					ProjectID: projectID,
					DryRun:    dryRun,
					Limit:     limit,
				})
				if err != nil {
					return 0, 0, err
				}
				return resp.CandidateCount, resp.Enqueued, nil
			},
			BackfillConsolidationEntities: func(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int) (int, int, error) {
				resp, err := enrichSvc.BackfillConsolidationEntities(ctx, &service.BackfillConsolidationEntitiesRequest{
					ProjectID: projectID,
					DryRun:    dryRun,
					Limit:     limit,
				})
				if err != nil {
					return 0, 0, err
				}
				return resp.CandidateCount, resp.Enqueued, nil
			},
			// The queue health surface is polled (~10s) and the count is a scan
			// over the memories table, so cache it briefly to keep the poll off a
			// repeated full scan; an informational indicator tolerates TTL staleness.
			CountMissingEmbeddings: cachedInt64(30*time.Second, memoryRepo.CountMissingEmbeddings),
			ClearCompletedJobs:     enrichmentAdminStore.ClearCompletedJobs,
		}),
		AdminOAuth:           api.NewAdminOAuthHandler(api.OAuthAdminConfig{Store: oauthAdminStore}),
		AdminWebhooks:        api.NewAdminWebhooksHandler(api.WebhookAdminConfig{Store: webhookAdminStore}),
		AdminAnalytics:       api.NewAdminAnalyticsHandler(api.AnalyticsConfig{Store: analyticsStore}),
		AdminUsage:           api.NewAdminUsageHandler(api.UsageConfig{Store: usageStore}),
		UsageCostRates:       api.NewUsageCostRatesHandler(api.CostRatesConfig{Store: settingsAdminStore}),
		AdminNamespaces:      api.NewAdminNamespacesHandler(api.NamespaceAdminConfig{Store: namespaceAdminStore}),
		AdminDatabase:        api.NewAdminDatabaseHandler(api.DatabaseAdminConfig{Store: databaseAdminStore}),
		AdminVectorMigration: api.NewAdminVectorMigrationHandler(api.VectorMigrationAdminConfig{Store: vectorMigrationStore}),
		AdminGraph: api.NewAdminGraphHandler(api.GraphAdminConfig{
			Projects:      projectRepo,
			Entities:      entityRepo,
			Relationships: relationshipRepo,
			Aliases:       entityAliasRepo,
			Namespaces:    namespaceRepo,
			Orgs:          orgRepo,
			Settings:      settingsSvc,
			Memories:      memoryRepo,
		}),
		AdminGraphMaintenance: api.NewAdminGraphMaintenanceHandler(api.GraphMaintenanceConfig{
			Maintainer: lifecycleSvc,
		}),
		AdminDreaming: api.NewAdminDreamingHandler(api.DreamAdminConfig{
			Store:    dreamAdminStore,
			Rollback: dreamRollback,
		}),
		AdminLogs: api.NewAdminLogsHandler(api.LogAdminConfig{Store: logEntryRepo}),

		// Tier-B (org-aggregate) handlers: caller must be RoleOrgOwner+
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
		OrgUsage: api.NewOrgUsageHandler(api.UsageConfig{Store: usageStore}),
		OrgDreaming: api.NewOrgDreamingHandler(api.OrgDreamingConfig{
			Store:    dreamAdminStore,
			Rollback: dreamRollback,
		}),
		OrgEnrichment: api.NewOrgEnrichmentHandler(api.OrgEnrichmentConfig{
			Store:             enrichmentAdminStore,
			ReExtractMemories: reExtractMemoriesFn,
		}),

		// Tier-C (system-aggregate) handlers: RoleAdministrator only via
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
		SystemUsage:    api.NewSystemUsageHandler(api.UsageConfig{Store: usageStore}),
		SystemIdentity: api.NewInstanceIdentityHandler(instanceIdentity),
		JWKS:           api.NewJWKSHandler(instanceIdentity),
	}

	// Build router config with auth middleware and rate limiter. Cleanup
	// and stale-after windows are read once from settings; runtime changes
	// require server restart.
	authMiddleware := auth.NewAuthMiddleware(apiKeyRepo, userRepo, jwtSecret, sessionTimings).
		WithShareTokens(shareTokenSvc, shareTokenRepo).
		WithClientUsage(oauthRepo)
	rateLimiter := auth.NewRateLimiter(10, 20,
		settingsSvc.ResolveDurationSecondsWithDefault(context.Background(),
			service.SettingAPIRateLimitCleanupSeconds, "global"),
		settingsSvc.ResolveDurationSecondsWithDefault(context.Background(),
			service.SettingAPIRateLimitStaleSeconds, "global"))
	defer rateLimiter.Stop()

	// Project access middleware enforces org-membership checks on all
	// /v1/projects/{project_id}/memories/* routes.
	routerCfg := server.RouterConfig{
		Metrics:        promMetrics,
		AuthMiddleware: authMiddleware,
		RateLimiter:    rateLimiter,
		SetupGuard:     api.SetupGuardMiddleware(setupChecker.IsComplete),
		ProjectAccess:  api.ProjectAccessMiddleware(projectAccessCfg),
		EnrichmentGate: api.EnrichmentGateMiddleware(enrichmentAvailable),
		// Resolved live per request so toggling ask.enabled in the admin UI
		// surfaces or hides the ask endpoints without a restart.
		AskGate: api.AskGateMiddleware(func(ctx context.Context) bool {
			return settingsSvc.ResolveBoolWithDefault(ctx, service.SettingAskEnabled, "global")
		}),
	}

	r := server.NewRouter(routerCfg, handlers)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// Run the server under the kardianos service runtime. Interactively this
	// installs a SIGINT/SIGTERM handler and drives Start/Stop (graceful
	// shutdown); under a service manager it connects to the Windows SCM,
	// systemd, or launchd. svc.Run blocks until Stop finishes, after which main
	// returns and the deferred context cancels and Close calls above fire in the
	// original teardown order.
	prg := &program{srv: srv, addr: addr, logLevel: cfg.LogLevel}
	svc, err := buildService(prg, os.Args)
	if err != nil {
		log.Fatalf("failed to build service runtime: %v", err)
	}
	if err := svc.Run(); err != nil {
		log.Fatalf("server runtime error: %v", err)
	}
}

// seedRegisteredSettings inserts one row per registered schema entry, using
// INSERT ... ON CONFLICT DO NOTHING so operator-set values survive untouched.
// Idempotent on every boot: every key the admin UI surfaces has a row.
func seedRegisteredSettings(ctx context.Context, repo *storage.SettingsRepo) error {
	schemas := adminstore.SettingsSchemas()
	defaults := make(map[string]string, len(schemas))
	for _, s := range schemas {
		defaults[s.Key] = string(s.DefaultValue)
	}
	return service.SeedSettingsDefaults(ctx, repo, defaults)
}

// logFusionStartupAudit emits one-shot deployment-time observations about
// the recall.fusion.* settings. The values themselves are read live per
// recall (see RecallService.resolveFusion); this helper only surfaces
// drift after an upgrade and outright misconfiguration that would silently
// produce zero candidates.
func logFusionStartupAudit(ctx context.Context, settingsSvc *service.SettingsService) {
	vec := service.DefaultFusionConfig.VectorWeight
	lex := service.DefaultFusionConfig.LexicalWeight
	if w, err := settingsSvc.ResolveFloat(ctx, service.SettingRecallFusionVecW, "global"); err == nil && w >= 0 {
		vec = w
	}
	if w, err := settingsSvc.ResolveFloat(ctx, service.SettingRecallFusionLexW, "global"); err == nil && w >= 0 {
		lex = w
	}

	// Tolerance 1e-9 sits well below any plausible UI slider resolution
	// (step:0.05 in the admin UI) and well above FP rounding noise from
	// strconv.ParseFloat round-trips, so this only fires when an operator
	// truly stored a different value.
	if !nearlyEqual(vec, service.DefaultFusionConfig.VectorWeight) ||
		!nearlyEqual(lex, service.DefaultFusionConfig.LexicalWeight) {
		slog.Info("recall.fusion weights differ from registered defaults",
			"stored_vector_weight", vec,
			"stored_lexical_weight", lex,
			"default_vector_weight", service.DefaultFusionConfig.VectorWeight,
			"default_lexical_weight", service.DefaultFusionConfig.LexicalWeight,
			"hint", "reset via /v1/admin/settings or the admin UI to adopt the new defaults",
		)
	}
	// Degenerate-fusion sanity check: if the resolved weights sum to zero
	// (or somehow go negative), every RRF score will collapse to zero,
	// runHybridSearch will emit an empty simMap, and recalls will silently
	// return zero candidates. Warn at boot so the operator sees the
	// misconfig before traffic hits. Live edits via the admin UI no longer
	// require a restart, but a deployment booting into this state still
	// deserves a loud signal.
	if vec+lex <= 0 {
		slog.Warn("recall.fusion weights sum to zero; fused recalls will return no candidates",
			"stored_vector_weight", vec,
			"stored_lexical_weight", lex,
			"hint", "set at least one of recall.fusion.vector_weight or recall.fusion.lexical_weight above zero",
		)
	}
}

// nearlyEqual compares two floats with a tolerance well below any meaningful
// admin-UI slider resolution (step:0.05) and well above FP rounding noise.
func nearlyEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

// cachedInt64 wraps a count function with a short in-process TTL cache so a
// polled health surface does not re-run an expensive count on every request.
// On a refresh error it serves the last good value (when one exists) rather than
// surfacing a transient zero. Safe for concurrent callers.
func cachedInt64(ttl time.Duration, fn func(context.Context) (int64, error)) func(context.Context) (int64, error) {
	var (
		mu     sync.Mutex
		val    int64
		at     time.Time
		cached bool
	)
	return func(ctx context.Context) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		if cached && time.Since(at) < ttl {
			return val, nil
		}
		n, err := fn(ctx)
		if err != nil {
			if cached {
				return val, nil
			}
			return 0, err
		}
		val, at, cached = n, time.Now(), true
		return n, nil
	}
}
