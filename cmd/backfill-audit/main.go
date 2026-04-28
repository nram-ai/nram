// Command backfill-audit drains the dream novelty-audit backlog for a
// single project without waiting on the dream scheduler, dirty tracker,
// cooldown, or min-interval. It connects to the same database and provider
// stack as the server (read from a config file), resolves the target
// project's namespace, and calls ConsolidationPhase.AuditExistingDreams
// until the requested cap is reached or no unaudited dreams remain.
//
// Intended for operator use only. A transient dream_cycles row is created
// for log attribution and marked completed when the run ends; the scheduler
// is not consulted and the project dirty flag is not altered.
//
// Usage:
//
//	backfill-audit --config=/path/to/config.yaml --project=<slug> [--max=2000]
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/dreaming"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

func main() {
	var (
		configPath  string
		projectSlug string
		maxAudits   int
		tokenBudget int
		perCallCap  int
		dryRun      bool
	)
	flag.StringVar(&configPath, "config", "config.yaml", "path to nram config file")
	flag.StringVar(&projectSlug, "project", "", "project slug whose backlog should be audited (required)")
	flag.IntVar(&maxAudits, "max", 2000, "maximum dreams to audit in this run")
	flag.IntVar(&tokenBudget, "budget", 500000, "total token budget for the run")
	flag.IntVar(&perCallCap, "per-call-cap", 10240, "per-LLM-call token cap")
	flag.BoolVar(&dryRun, "dry-run", false, "report eligible count only; do not audit")
	flag.Parse()

	if projectSlug == "" {
		fmt.Fprintln(os.Stderr, "required flag: --project=<slug>")
		flag.Usage()
		os.Exit(2)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	db, err := storage.Open(cfg.Database)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()
	log.Printf("database backend: %s", db.Backend())

	memoryRepo := storage.NewMemoryRepo(db)
	projectRepo := storage.NewProjectRepo(db)
	lineageRepo := storage.NewMemoryLineageRepo(db)
	settingsRepo := storage.NewSettingsRepo(db)
	settingsSvc := service.NewSettingsService(settingsRepo)

	// Build providers from config file values. The admin UI overlays these
	// at runtime via the settings table, but the CLI skips that overlay —
	// operators can export live values into the config if they need them.
	regCfg := provider.RegistryConfig{
		Embedding: provider.SlotConfig{Type: cfg.Embed.Provider, BaseURL: cfg.Embed.URL, APIKey: cfg.Embed.Key, Model: cfg.Embed.Model},
		Fact:      provider.SlotConfig{Type: cfg.Fact.Provider, BaseURL: cfg.Fact.URL, APIKey: cfg.Fact.Key, Model: cfg.Fact.Model},
		Entity:    provider.SlotConfig{Type: cfg.Entity.Provider, BaseURL: cfg.Entity.URL, APIKey: cfg.Entity.Key, Model: cfg.Entity.Model},
	}
	registry, err := provider.NewRegistry(regCfg, nil, nil)
	if err != nil {
		log.Fatalf("provider registry init: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	project, err := findProjectBySlug(ctx, projectRepo, projectSlug)
	if err != nil {
		log.Fatalf("resolve project %q: %v", projectSlug, err)
	}
	log.Printf("target project: slug=%s id=%s namespace=%s", project.Slug, project.ID, project.NamespaceID)

	memories, err := listAllMemories(ctx, memoryRepo, project.NamespaceID)
	if err != nil {
		log.Fatalf("list memories: %v", err)
	}
	eligible := countUnauditedDreams(memories)
	log.Printf("eligible (unaudited dream) memories: %d / total: %d", eligible, len(memories))

	if dryRun || eligible == 0 {
		log.Println("dry-run or no eligible memories; exiting")
		return
	}

	consolidationPhase := dreaming.NewConsolidationPhase(
		memoryRepo, memoryRepo, lineageRepo,
		func() provider.LLMProvider { return registry.GetFact() },
		func() provider.EmbeddingProvider { return registry.GetEmbedding() },
		settingsSvc,
	)

	cycle := &model.DreamCycle{
		ID:          uuid.New(),
		ProjectID:   project.ID,
		NamespaceID: project.NamespaceID,
		Status:      model.DreamStatusPending,
		TokenBudget: tokenBudget,
	}
	dreamCycleRepo := storage.NewDreamCycleRepo(db)
	if err := dreamCycleRepo.Create(ctx, cycle); err != nil {
		log.Fatalf("create transient dream cycle: %v", err)
	}
	if err := dreamCycleRepo.Start(ctx, cycle.ID); err != nil {
		log.Fatalf("start transient dream cycle: %v", err)
	}

	dreamLogRepo := storage.NewDreamLogRepo(db)
	logger := dreaming.NewDreamLogWriter(dreamLogRepo, cycle.ID, cycle.ProjectID)
	budget := dreaming.NewTokenBudget(tokenBudget, perCallCap)

	runCap := maxAudits
	if runCap > eligible {
		runCap = eligible
	}
	log.Printf("starting audit: cap=%d budget=%d per_call=%d", runCap, tokenBudget, perCallCap)

	start := time.Now()
	residual, err := consolidationPhase.AuditExistingDreams(ctx, cycle, budget, logger, registry.GetFact(), memories, runCap)
	elapsed := time.Since(start)

	if cerr := dreamCycleRepo.Complete(ctx, cycle.ID, []byte(`[{"phase":"backfill_audit_cli"}]`), budget.Used()); cerr != nil {
		log.Printf("warning: mark cycle complete failed: %v", cerr)
	}

	if err != nil {
		log.Fatalf("backlog audit error after %s: %v", elapsed, err)
	}

	// residual=true means the cap was hit with more eligible memories still
	// unaudited; residual=false means everything in this run was visited.
	// The exact number demoted vs stamped is persisted to dream_logs for
	// the cycle, reachable via the UI or a direct DB read.
	log.Printf("done in %s: tokens_used=%d cap=%d eligible_before=%d residual=%t",
		elapsed, budget.Used(), runCap, eligible, residual)
}

// findProjectBySlug iterates all projects (system-level; does not filter
// by user permissions) and returns the first slug match.
func findProjectBySlug(ctx context.Context, repo *storage.ProjectRepo, slug string) (*model.Project, error) {
	projects, err := repo.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	for i := range projects {
		if projects[i].Slug == slug {
			return &projects[i], nil
		}
	}
	return nil, errors.New("no project matches slug")
}

// listAllMemories paginates ListByNamespace until exhausted.
func listAllMemories(ctx context.Context, repo *storage.MemoryRepo, nsID uuid.UUID) ([]model.Memory, error) {
	const page = 1000
	var all []model.Memory
	offset := 0
	for {
		batch, err := repo.ListByNamespace(ctx, nsID, page, offset)
		if err != nil {
			return nil, err
		}
		all = append(all, batch...)
		if len(batch) < page {
			break
		}
		offset += page
	}
	return all, nil
}

// countUnauditedDreams counts non-deleted dream-source memories whose
// metadata lacks the novelty audit stamp.
func countUnauditedDreams(memories []model.Memory) int {
	stamp := []byte(dreaming.NoveltyAuditStampKey)
	count := 0
	for i := range memories {
		m := &memories[i]
		if m.DeletedAt != nil || model.MemorySource(m) != model.DreamSource {
			continue
		}
		if bytes.Contains(m.Metadata, stamp) {
			continue
		}
		count++
	}
	return count
}
