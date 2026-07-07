package dreaming

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/periodic"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// retentionSweepInterval is how often dream-log retention runs. Kept a constant
// (not a setting) because no dreaming.*_sweep_interval setting exists; the
// operator-tunable knob is the retention window (dreaming.log_retention_days),
// not this cadence.
const retentionSweepInterval = 6 * time.Hour

// ProjectReader looks up project details.
type ProjectReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// retentionSweeper is the subset of *RetentionSweeper the scheduler drives on
// its retention loop. Consumer-side interface so tests can substitute a fake
// without standing up the full sweeper and its repos.
type retentionSweeper interface {
	Sweep(ctx context.Context) error
}

// SchedulerConfig configures the dream scheduler.
type SchedulerConfig struct {
	// PollInterval is how often the scheduler checks for eligible projects.
	// Zero falls through to SettingDreamSchedulerPollSeconds; read once at
	// scheduler start so changes require a server restart.
	PollInterval time.Duration
	// EnrichmentAvailable returns true iff embedding, fact, and entity
	// providers are all configured. The scheduler skips its poll when this
	// returns false so dream cycles never run with a missing slot. Read
	// live each poll so a provider reload reopens the gate without
	// restarting the process. Nil means the gate is always open (used in
	// tests that don't exercise it).
	EnrichmentAvailable func() bool
}

func (c SchedulerConfig) withDefaults(ctx context.Context, settings SettingsResolver) SchedulerConfig {
	if c.PollInterval <= 0 {
		if settings != nil {
			c.PollInterval = settings.ResolveDurationSecondsWithDefault(ctx,
				service.SettingDreamSchedulerPollSeconds, "global")
		}
		if c.PollInterval < time.Second {
			c.PollInterval = time.Second
		}
	}
	return c
}

// Scheduler monitors for eligible projects and triggers dream cycles.
// It runs a single dream cycle at a time per instance.
type Scheduler struct {
	config    SchedulerConfig
	settings  SettingsResolver
	cascade   CascadeResolver
	dirtyRepo *storage.DreamDirtyRepo
	cycleRepo *storage.DreamCycleRepo
	projects  ProjectReader
	idleCheck IdleChecker
	runner    *Runner
	eventBus  events.EventBus
	retention retentionSweeper

	// activeCycles tracks in-flight cycles owned by this instance, keyed by
	// cycle ID. Used by CancelCycle so an admin Abandon hitting THIS instance
	// can interrupt the running ctx mid-phase rather than waiting for the
	// next phase boundary. Cross-instance Abandon falls back to the DB write
	// alone; the remote runner notices on its next phase boundary.
	activeCycles   map[uuid.UUID]context.CancelFunc
	activeCyclesMu sync.Mutex

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewScheduler creates a new dream scheduler. cascade is required: it
// resolves per-project dreaming_enabled overrides through the same path
// project/user settings PATCHes write, so a UI toggle takes effect without
// any direct settings-table writes from the dreaming subsystem.
func NewScheduler(
	config SchedulerConfig,
	settings SettingsResolver,
	cascade CascadeResolver,
	dirtyRepo *storage.DreamDirtyRepo,
	cycleRepo *storage.DreamCycleRepo,
	projects ProjectReader,
	idleCheck IdleChecker,
	runner *Runner,
	eventBus events.EventBus,
	retention retentionSweeper,
) *Scheduler {
	return &Scheduler{
		config:       config.withDefaults(context.Background(), settings),
		settings:     settings,
		cascade:      cascade,
		dirtyRepo:    dirtyRepo,
		cycleRepo:    cycleRepo,
		projects:     projects,
		idleCheck:    idleCheck,
		runner:       runner,
		eventBus:     eventBus,
		retention:    retention,
		activeCycles: make(map[uuid.UUID]context.CancelFunc),
	}
}

// CancelCycle cancels the in-flight ctx for a cycle owned by this instance.
// Returns true if the cycle was registered locally (and thus actually canceled),
// false if the cycle is owned by a different instance or has already completed.
// The caller must still write the DB row's terminal state separately;
// canceling the ctx alone does not transition the cycle's status.
func (s *Scheduler) CancelCycle(id uuid.UUID) bool {
	s.activeCyclesMu.Lock()
	defer s.activeCyclesMu.Unlock()

	cancel, ok := s.activeCycles[id]
	if !ok {
		return false
	}
	cancel()
	delete(s.activeCycles, id)
	return true
}

func (s *Scheduler) registerCycle(id uuid.UUID, cancel context.CancelFunc) {
	s.activeCyclesMu.Lock()
	defer s.activeCyclesMu.Unlock()
	s.activeCycles[id] = cancel
}

func (s *Scheduler) unregisterCycle(id uuid.UUID) {
	s.activeCyclesMu.Lock()
	defer s.activeCyclesMu.Unlock()
	delete(s.activeCycles, id)
}

// Start launches the scheduler in a background goroutine, plus a second
// goroutine for the dream-log retention sweep. Retention runs on its own
// periodic.Run loop (rather than a branch of the poll select) so it sweeps once
// at startup — a restart reclaims already-expired dream logs immediately
// instead of waiting up to a full retention interval. Both goroutines share the
// scheduler's ctx and waitgroup, so Stop tears them both down.
func (s *Scheduler) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.run(ctx)

	// retention is always non-nil in production (NewScheduler wires it); the
	// guard only covers Schedulers built via struct literal in tests that omit
	// it and never exercise the retention path.
	if s.retention != nil {
		s.wg.Go(func() {
			periodic.Run(ctx, periodic.Fixed(retentionSweepInterval),
				func(ctx context.Context, startup bool) {
					if err := s.retention.Sweep(ctx); err != nil {
						if startup {
							slog.Warn("dreaming: startup retention sweep failed", "err", err)
						} else {
							slog.Warn("dreaming: retention sweep failed", "err", err)
						}
					}
				})
		})
	}
}

// Stop cancels the scheduler and waits for it to finish. Any cycle that was
// in-flight at shutdown is explicitly abandoned with a fresh context so its
// DB row reflects the shutdown rather than being left as 'running' for the
// stuck sweeper on the next instance to catch 30 minutes later. SIGKILL
// bypasses this path; the sweeper still backs it up.
func (s *Scheduler) Stop() {
	// Capture before cancel: the runCycle goroutine's defer unregisters its
	// own entry as it exits, so reading after wg.Wait() always finds the map
	// empty.
	inflight := s.snapshotActiveCycles()

	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	if len(inflight) == 0 || s.cycleRepo == nil {
		return
	}

	// Fresh context so the canceled-during-shutdown ctx doesn't fail the
	// terminal write. Short timeout so a hung DB can't delay process exit.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, id := range inflight {
		ok, err := s.cycleRepo.Abandon(ctx, id, "server shutdown; cycle interrupted by graceful stop")
		if err != nil {
			slog.Warn("dreaming: failed to abandon cycle on shutdown",
				"cycle", id, "err", err)
			continue
		}
		if ok {
			slog.Info("dreaming: abandoned in-flight cycle on shutdown", "cycle", id)
		}
	}
}

func (s *Scheduler) snapshotActiveCycles() []uuid.UUID {
	s.activeCyclesMu.Lock()
	defer s.activeCyclesMu.Unlock()
	ids := make([]uuid.UUID, 0, len(s.activeCycles))
	for id := range s.activeCycles {
		ids = append(ids, id)
	}
	return ids
}

func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.config.PollInterval):
			s.poll(ctx)
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

	// Resolve timing constraints. ResolveIntWithDefault keeps the missing-key
	// case in lockstep with settingDefaults; hardcoded literals here would
	// drift the moment defaults change in service.settings. The explicit
	// floor-on-zero preserves the pre-refactor behavior for deployments that
	// have a stored value of 0 (the schema's Min=0 makes that a legal write):
	// 0 was always treated as "use the registered default" so a stale or
	// hand-edited row cannot disable the cooldown / min-interval and let a
	// runaway scheduler dream on every poll. An operator who wants near-zero
	// timing can still set 1.
	cooldownSecs := s.settings.ResolveIntWithDefault(ctx, service.SettingDreamCooldown, "global")
	if cooldownSecs <= 0 {
		cooldownSecs = service.GetDefaultInt(service.SettingDreamCooldown)
	}
	minIntervalSecs := s.settings.ResolveIntWithDefault(ctx, service.SettingDreamMinInterval, "global")
	if minIntervalSecs <= 0 {
		minIntervalSecs = service.GetDefaultInt(service.SettingDreamMinInterval)
	}
	cooldown := time.Duration(cooldownSecs) * time.Second
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

	// Wrap ctx so CancelCycle can interrupt this cycle mid-phase without
	// canceling the whole scheduler. Registry entry is removed in defer
	// regardless of how Execute returns; CancelCycle also deletes on cancel
	// to make a duplicate cancel idempotent.
	cycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.registerCycle(cycle.ID, cancel)
	defer s.unregisterCycle(cycle.ID)

	budget := NewTokenBudget(maxTokens, maxPerCall)
	allCompleted, hasResidual, err := s.runner.Execute(cycleCtx, cycle, budget)

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
	// Cascade composes global dreaming.enabled with the project's
	// settings.dreaming_enabled override. nil-cascade is a test path:
	// fall back to the global flag rather than the opt-out default.
	if s.cascade != nil {
		return s.cascade.ResolveDreamingEnabled(ctx, project.NamespaceID)
	}
	val, _ := s.settings.Resolve(ctx, service.SettingDreamingEnabled, "global")
	return val == "true" || val == "1"
}
