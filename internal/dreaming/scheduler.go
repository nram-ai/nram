package dreaming

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ProjectReader looks up project details.
type ProjectReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// SchedulerConfig configures the dream scheduler.
type SchedulerConfig struct {
	// PollInterval is how often the scheduler checks for eligible projects.
	PollInterval time.Duration
	// EnrichmentAvailable returns true iff embedding, fact, and entity
	// providers are all configured. The scheduler skips its poll when this
	// returns false so dream cycles never run with a missing slot. Read
	// live each poll so a provider reload reopens the gate without
	// restarting the process. Nil means the gate is always open (used in
	// tests that don't exercise it).
	EnrichmentAvailable func() bool
}

func (c SchedulerConfig) withDefaults() SchedulerConfig {
	if c.PollInterval <= 0 {
		c.PollInterval = 30 * time.Second
	}
	return c
}

// Scheduler monitors for eligible projects and triggers dream cycles.
// It runs a single dream cycle at a time per instance.
type Scheduler struct {
	config    SchedulerConfig
	settings  SettingsResolver
	dirtyRepo *storage.DreamDirtyRepo
	cycleRepo *storage.DreamCycleRepo
	projects  ProjectReader
	idleCheck IdleChecker
	runner    *Runner
	eventBus  events.EventBus
	retention *RetentionSweeper

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewScheduler creates a new dream scheduler.
func NewScheduler(
	config SchedulerConfig,
	settings SettingsResolver,
	dirtyRepo *storage.DreamDirtyRepo,
	cycleRepo *storage.DreamCycleRepo,
	projects ProjectReader,
	idleCheck IdleChecker,
	runner *Runner,
	eventBus events.EventBus,
	retention *RetentionSweeper,
) *Scheduler {
	return &Scheduler{
		config:    config.withDefaults(),
		settings:  settings,
		dirtyRepo: dirtyRepo,
		cycleRepo: cycleRepo,
		projects:  projects,
		idleCheck: idleCheck,
		runner:    runner,
		eventBus:  eventBus,
		retention: retention,
	}
}

// Start launches the scheduler in a background goroutine.
func (s *Scheduler) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.run(ctx)
}

// Stop cancels the scheduler and waits for it to finish.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()

	retentionTicker := time.NewTicker(6 * time.Hour)
	defer retentionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.config.PollInterval):
			s.poll(ctx)
		case <-retentionTicker.C:
			if err := s.retention.Sweep(ctx); err != nil {
				slog.Warn("dreaming: retention sweep failed", "err", err)
			}
		}
	}
}

func (s *Scheduler) poll(ctx context.Context) {
	if s.config.EnrichmentAvailable != nil && !s.config.EnrichmentAvailable() {
		return
	}

	// Check global dream enable.
	enabledStr, _ := s.settings.Resolve(ctx, service.SettingDreamingEnabled, "global")
	if enabledStr != "true" && enabledStr != "1" {
		return
	}

	// Check enrichment is idle.
	if s.idleCheck != nil && !s.idleCheck.IsIdle() {
		return
	}

	// Resolve timing constraints.
	cooldownSecs, _ := s.settings.ResolveInt(ctx, service.SettingDreamCooldown, "global")
	if cooldownSecs <= 0 {
		cooldownSecs = 300
	}
	cooldown := time.Duration(cooldownSecs) * time.Second

	minIntervalSecs, _ := s.settings.ResolveInt(ctx, service.SettingDreamMinInterval, "global")
	if minIntervalSecs <= 0 {
		minIntervalSecs = 3600
	}
	minInterval := time.Duration(minIntervalSecs) * time.Second

	// Get dirty projects.
	dirtyProjects, err := s.dirtyRepo.ListDirtyProjects(ctx)
	if err != nil {
		slog.Error("dreaming: failed to list dirty projects", "err", err)
		return
	}

	now := time.Now().UTC()

	for _, dp := range dirtyProjects {
		// Re-check idle between projects.
		if s.idleCheck != nil && !s.idleCheck.IsIdle() {
			slog.Info("dreaming: enrichment active, pausing scheduler")
			return
		}

		// Re-check the global enable flag between projects so operators
		// can quiesce the scheduler mid-poll ahead of a deploy without
		// having to wait out every dirty project started by this poll.
		enabledStr, _ := s.settings.Resolve(ctx, service.SettingDreamingEnabled, "global")
		if enabledStr != "true" && enabledStr != "1" {
			slog.Info("dreaming: disabled mid-poll, stopping scheduler loop")
			return
		}

		if ctx.Err() != nil {
			return
		}

		// Check cooldown: project must have been idle for at least cooldown duration.
		if now.Sub(dp.DirtySince) < cooldown {
			continue
		}

		// Check min interval since last dream.
		if dp.LastDreamAt != nil && now.Sub(*dp.LastDreamAt) < minInterval {
			continue
		}

		// Check project-level dream enable.
		project, err := s.projects.GetByID(ctx, dp.ProjectID)
		if err != nil {
			slog.Warn("dreaming: project not found", "project", dp.ProjectID, "err", err)
			continue
		}

		if !s.isProjectDreamingEnabled(ctx, project) {
			continue
		}

		// Run dream cycle for this project.
		s.runCycle(ctx, project)
	}
}

func (s *Scheduler) runCycle(ctx context.Context, project *model.Project) {
	// Resolve token budget.
	maxTokens, _ := s.settings.ResolveInt(ctx, service.SettingDreamMaxTokensPerCycle, "global")
	if maxTokens <= 0 {
		maxTokens = 10000
	}
	maxPerCall, _ := s.settings.ResolveInt(ctx, service.SettingDreamMaxTokensPerCall, "global")
	if maxPerCall <= 0 {
		maxPerCall = 2048
	}

	cycle := &model.DreamCycle{
		ID:          uuid.New(),
		ProjectID:   project.ID,
		NamespaceID: project.NamespaceID,
		Status:      model.DreamStatusPending,
		TokenBudget: maxTokens,
	}

	if err := s.cycleRepo.Create(ctx, cycle); err != nil {
		slog.Error("dreaming: failed to create cycle", "project", project.ID, "err", err)
		return
	}

	// Emit cycle started event.
	events.Emit(ctx, s.eventBus, events.DreamCycleStarted, "project:"+project.ID.String(),
		map[string]string{
			"cycle_id":   cycle.ID.String(),
			"project_id": project.ID.String(),
		})

	slog.Info("dreaming: starting cycle", "cycle", cycle.ID, "project", project.Slug)

	budget := NewTokenBudget(maxTokens, maxPerCall)
	allCompleted, hasResidual, err := s.runner.Execute(ctx, cycle, budget)

	if err != nil {
		slog.Error("dreaming: cycle failed", "cycle", cycle.ID, "err", err)
		events.Emit(ctx, s.eventBus, events.DreamCycleFailed, "project:"+project.ID.String(),
			map[string]string{
				"cycle_id":   cycle.ID.String(),
				"project_id": project.ID.String(),
				"error":      err.Error(),
			})
	} else {
		slog.Info("dreaming: cycle completed", "cycle", cycle.ID,
			"tokens_used", budget.Used(), "all_phases", allCompleted, "has_residual", hasResidual)
		events.Emit(ctx, s.eventBus, events.DreamCycleCompleted, "project:"+project.ID.String(),
			map[string]string{
				"cycle_id":   cycle.ID.String(),
				"project_id": project.ID.String(),
			})
	}

	// Clear dirty only when every phase ran AND no phase reported residual
	// work. A phase that completes its Execute call but hit a bounded batch
	// (e.g. novelty backfill per-cycle cap) leaves unfinished work behind,
	// and the scheduler needs to keep the project eligible for the next
	// cycle so it can drain.
	if allCompleted && !hasResidual {
		if err := s.dirtyRepo.ClearDirty(ctx, project.ID); err != nil {
			slog.Error("dreaming: failed to clear dirty flag", "project", project.ID, "err", err)
		}
	}
	if err := s.dirtyRepo.SetLastDreamAt(ctx, project.ID, time.Now().UTC()); err != nil {
		slog.Error("dreaming: failed to set last dream time", "project", project.ID, "err", err)
	}
}

func (s *Scheduler) isProjectDreamingEnabled(ctx context.Context, project *model.Project) bool {
	val, err := s.settings.Resolve(ctx, service.SettingDreamProjectEnabled, "project:"+project.ID.String())
	if err != nil || val == "" {
		return true // default: enabled (opt-out)
	}
	return val == "true" || val == "1"
}
