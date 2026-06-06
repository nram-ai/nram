package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// DreamCycleCanceller cancels in-flight cycles owned by the local Scheduler
// instance. Cross-instance cancellation falls back to the DB write alone, so
// this dependency is optional (nil-tolerant).
type DreamCycleCanceller interface {
	CancelCycle(id uuid.UUID) bool
}

// DreamSettingsResolver resolves the timing settings used to decorate
// running cycles with IsAbandonable / IsStaleDiagnostic flags. The store
// uses ResolveIntWithDefault rather than directly reading the settings repo
// so that the cache, default fallback, and value parsing are all handled
// once in service.SettingsService.
type DreamSettingsResolver interface {
	ResolveIntWithDefault(ctx context.Context, key, scope string) int
}

// DreamProjectReader looks up a project so ProjectStatus can resolve the
// project's namespace for cascade-based dreaming-enabled lookups.
type DreamProjectReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// DreamingResolver returns the effective dreaming-enabled flag for a
// namespace by composing the global setting with any per-project / per-user
// override stored in Settings JSON. Implemented by service.CascadeResolver.
type DreamingResolver interface {
	ResolveDreamingEnabled(ctx context.Context, namespaceID uuid.UUID) bool
}

// DreamAdminStore provides admin-level access to dream cycle data.
// It implements api.DreamAdminStore.
type DreamAdminStore struct {
	cycleRepo    *storage.DreamCycleRepo
	logRepo      *storage.DreamLogRepo
	dirtyRepo    *storage.DreamDirtyRepo
	settingsRepo *storage.SettingsRepo
	settings     DreamSettingsResolver
	canceller    DreamCycleCanceller
	projects     DreamProjectReader
	cascade      DreamingResolver
	db           storage.DB
}

// NewDreamAdminStore creates a new DreamAdminStore. canceller may be nil
// during tests or migrations that don't run a live scheduler; abandon then
// degrades to a pure DB write that the in-process runner picks up at the
// next phase boundary. projects + cascade are both required for ProjectStatus
// to report the same enabled flag the scheduler observes.
func NewDreamAdminStore(
	cycleRepo *storage.DreamCycleRepo,
	logRepo *storage.DreamLogRepo,
	dirtyRepo *storage.DreamDirtyRepo,
	settingsRepo *storage.SettingsRepo,
	settings DreamSettingsResolver,
	canceller DreamCycleCanceller,
	projects DreamProjectReader,
	cascade DreamingResolver,
	db storage.DB,
) *DreamAdminStore {
	return &DreamAdminStore{
		cycleRepo:    cycleRepo,
		logRepo:      logRepo,
		dirtyRepo:    dirtyRepo,
		settingsRepo: settingsRepo,
		settings:     settings,
		canceller:    canceller,
		projects:     projects,
		cascade:      cascade,
		db:           db,
	}
}

// thresholds caches the two timing settings for the duration of a single
// request so each cycle in a batch doesn't trigger its own settings lookup.
type thresholds struct {
	stuck     time.Duration
	heartbeat time.Duration
}

func (s *DreamAdminStore) resolveThresholds(ctx context.Context) thresholds {
	stuckSecs := s.settings.ResolveIntWithDefault(ctx, service.SettingDreamStuckThreshold, "global")
	if stuckSecs <= 0 {
		stuckSecs = 1800
	}
	hbSecs := s.settings.ResolveIntWithDefault(ctx, service.SettingDreamHeartbeatStale, "global")
	if hbSecs <= 0 {
		hbSecs = 120
	}
	return thresholds{
		stuck:     time.Duration(stuckSecs) * time.Second,
		heartbeat: time.Duration(hbSecs) * time.Second,
	}
}

// decorate stamps the computed IsAbandonable and IsStaleDiagnostic fields on
// a cycle. Both flags fire only for status='running' rows; everything else
// keeps the zero value the repo scan returned.
func decorate(c *model.DreamCycle, t thresholds, now time.Time) {
	if c.Status != model.DreamStatusRunning {
		return
	}
	if now.Sub(c.UpdatedAt) > t.stuck {
		c.IsAbandonable = true
	}
	if c.HeartbeatAt != nil && now.Sub(*c.HeartbeatAt) > t.heartbeat {
		c.IsStaleDiagnostic = true
	}
}

func (s *DreamAdminStore) decorateAllWith(cycles []model.DreamCycle, t thresholds) {
	if len(cycles) == 0 {
		return
	}
	now := time.Now().UTC()
	for i := range cycles {
		decorate(&cycles[i], t, now)
	}
}

func (s *DreamAdminStore) decorateAll(ctx context.Context, cycles []model.DreamCycle) {
	if len(cycles) == 0 {
		return
	}
	s.decorateAllWith(cycles, s.resolveThresholds(ctx))
}

// Status returns the system-wide dream status.
func (s *DreamAdminStore) Status(ctx context.Context) (*api.DreamStatusResponse, error) {
	dirtyCount, _ := s.dirtyRepo.CountDirty(ctx)
	cycles, _ := s.cycleRepo.ListRecent(ctx, 10)
	if cycles == nil {
		cycles = []model.DreamCycle{}
	}

	// Resolve once; the UI polls Status every 10 seconds, so we avoid a
	// second settings cache lookup and (more importantly) a second query
	// just to count stuck rows that the recent-cycles preview already
	// covers in the typical case.
	t := s.resolveThresholds(ctx)
	s.decorateAllWith(cycles, t)

	// CountStale gives an exact count without materializing a slice for
	// the rare case where stuck cycles exist beyond the 10-row preview
	// (post-deploy with many crashed workers).
	stuckCount, _ := s.cycleRepo.CountStale(ctx, t.stuck)

	return &api.DreamStatusResponse{
		Enabled:      s.isEnabled(ctx),
		DirtyCount:   dirtyCount,
		StuckCount:   stuckCount,
		RecentCycles: cycles,
	}, nil
}

// ProjectStatus returns the dream status for a specific project. The
// Enabled flag mirrors what the scheduler sees (cascade resolver: global
// dreaming.enabled merged with the project's own Settings.dreaming_enabled
// override). If the project lookup fails (e.g. the row was deleted between
// the caller's request and this query), Enabled is reported as false rather
// than synthesized from a partial cascade.
func (s *DreamAdminStore) ProjectStatus(ctx context.Context, projectID uuid.UUID) (*api.DreamProjectStatusResponse, error) {
	dirty, _ := s.dirtyRepo.IsDirty(ctx, projectID)
	cycles, _ := s.cycleRepo.ListByProject(ctx, projectID, 10)
	if cycles == nil {
		cycles = []model.DreamCycle{}
	}
	s.decorateAll(ctx, cycles)

	var lastDream *model.DreamCycle
	if len(cycles) > 0 {
		lastDream = &cycles[0]
	}

	enabled := false
	if s.projects != nil && s.cascade != nil {
		if proj, err := s.projects.GetByID(ctx, projectID); err == nil && proj != nil {
			enabled = s.cascade.ResolveDreamingEnabled(ctx, proj.NamespaceID)
		}
	}

	return &api.DreamProjectStatusResponse{
		Enabled:   enabled,
		Dirty:     dirty,
		LastDream: lastDream,
		Cycles:    cycles,
	}, nil
}

// ListCycles returns dream cycles, optionally filtered by project.
func (s *DreamAdminStore) ListCycles(ctx context.Context, projectID *uuid.UUID, limit int) ([]model.DreamCycle, error) {
	var (
		cycles []model.DreamCycle
		err    error
	)
	if projectID != nil {
		cycles, err = s.cycleRepo.ListByProject(ctx, *projectID, limit)
	} else {
		cycles, err = s.cycleRepo.ListRecent(ctx, limit)
	}
	if err != nil {
		return nil, err
	}
	s.decorateAll(ctx, cycles)
	return cycles, nil
}

// ListSelfCycles returns cycles whose project namespace is equal to or
// descended from callerNS.Path. Each row's ProjectName is populated by the
// underlying repo via the projects JOIN; self-tier callers own every
// returned project so the name is theirs to see.
func (s *DreamAdminStore) ListSelfCycles(ctx context.Context, callerNS *model.Namespace, limit int) ([]model.DreamCycle, error) {
	if callerNS == nil {
		return []model.DreamCycle{}, nil
	}
	cycles, err := s.cycleRepo.ListByNamespacePathPrefix(ctx, callerNS.Path, limit, true)
	if err != nil {
		return nil, err
	}
	s.decorateAll(ctx, cycles)
	return cycles, nil
}

// SelfDreamingDirtyCount returns the number of caller-owned projects with
// pending user-originated changes.
func (s *DreamAdminStore) SelfDreamingDirtyCount(ctx context.Context, callerNS *model.Namespace) (int, error) {
	if callerNS == nil {
		return 0, nil
	}
	return s.dirtyRepo.CountDirtyByNamespacePathPrefix(ctx, callerNS.Path)
}

// GetCycleLogs returns the log entries for a specific cycle.
func (s *DreamAdminStore) GetCycleLogs(ctx context.Context, cycleID uuid.UUID) ([]model.DreamLog, error) {
	return s.logRepo.ListByCycle(ctx, cycleID)
}

// GetCycle returns a specific dream cycle by ID.
func (s *DreamAdminStore) GetCycle(ctx context.Context, cycleID uuid.UUID) (*model.DreamCycle, error) {
	c, err := s.cycleRepo.GetByID(ctx, cycleID)
	if err != nil {
		return nil, err
	}
	t := s.resolveThresholds(ctx)
	decorate(c, t, time.Now().UTC())
	return c, nil
}

// AbandonCycle transitions a running cycle to failed. If the cycle is owned
// by the local scheduler, its in-flight ctx is canceled first so the runner
// stops at its next ctx-aware checkpoint instead of finishing the current
// phase. Cross-instance cycles get only the DB write; the remote runner
// notices on its next phase boundary. Returns true iff a row was actually
// transitioned (false means it was already terminal).
func (s *DreamAdminStore) AbandonCycle(ctx context.Context, cycleID uuid.UUID, reason string) (bool, error) {
	if s.canceller != nil {
		s.canceller.CancelCycle(cycleID)
	}
	return s.cycleRepo.Abandon(ctx, cycleID, reason)
}

// SetEnabled sets the global dreaming enabled state.
func (s *DreamAdminStore) SetEnabled(ctx context.Context, enabled bool) error {
	val := "false"
	if enabled {
		val = "true"
	}
	value, _ := json.Marshal(val)
	return s.settingsRepo.Set(ctx, &model.Setting{
		Key:   service.SettingDreamingEnabled,
		Value: json.RawMessage(value),
		Scope: "global",
	})
}

// orgNamespacePath returns the org's root namespace path.
func (s *DreamAdminStore) orgNamespacePath(ctx context.Context, orgID uuid.UUID) (string, error) {
	q := "SELECT n.path FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = ?"
	if s.db.Backend() == storage.BackendPostgres {
		q = "SELECT n.path FROM namespaces n JOIN organizations o ON o.namespace_id = n.id WHERE o.id = $1"
	}
	var p string
	row := s.db.QueryRow(ctx, q, orgID.String())
	if err := row.Scan(&p); err != nil {
		return "", fmt.Errorf("org namespace path: %w", err)
	}
	return p, nil
}

// OrgListCycles returns dream cycles whose project namespace is descended
// from (or equal to) the org's root namespace path. ProjectName is left
// empty on each row so an org_owner sees project_id only for projects
// owned by other users in the org, matching the system-tier privacy
// posture and the rule documented on model.DreamCycle.
func (s *DreamAdminStore) OrgListCycles(ctx context.Context, orgID uuid.UUID, limit int) ([]model.DreamCycle, error) {
	orgPath, err := s.orgNamespacePath(ctx, orgID)
	if err != nil {
		return nil, err
	}
	cycles, err := s.cycleRepo.ListByNamespacePathPrefix(ctx, orgPath, limit, false)
	if err != nil {
		return nil, err
	}
	s.decorateAll(ctx, cycles)
	return cycles, nil
}

// OrgDirtyCount returns the number of org-owned projects with pending
// user-originated changes.
func (s *DreamAdminStore) OrgDirtyCount(ctx context.Context, orgID uuid.UUID) (int, error) {
	orgPath, err := s.orgNamespacePath(ctx, orgID)
	if err != nil {
		return 0, err
	}
	return s.dirtyRepo.CountDirtyByNamespacePathPrefix(ctx, orgPath)
}

// OrgStuckCount returns the number of running cycles in the org that have
// crossed the stuck threshold.
func (s *DreamAdminStore) OrgStuckCount(ctx context.Context, orgID uuid.UUID) (int, error) {
	orgPath, err := s.orgNamespacePath(ctx, orgID)
	if err != nil {
		return 0, err
	}
	t := s.resolveThresholds(ctx)
	return s.cycleRepo.CountStaleByNamespacePathPrefix(ctx, orgPath, t.stuck)
}

// projectNamespacePath returns the namespace path for the given project id.
func (s *DreamAdminStore) projectNamespacePath(ctx context.Context, projectID uuid.UUID) (string, error) {
	q := `SELECT n.path FROM namespaces n JOIN projects p ON p.namespace_id = n.id WHERE p.id = ?`
	if s.db.Backend() == storage.BackendPostgres {
		q = `SELECT n.path FROM namespaces n JOIN projects p ON p.namespace_id = n.id WHERE p.id = $1`
	}
	var p string
	row := s.db.QueryRow(ctx, q, projectID.String())
	if err := row.Scan(&p); err != nil {
		return "", fmt.Errorf("project namespace path: %w", err)
	}
	return p, nil
}

// CycleInOrg returns true iff the cycle's project namespace is in the org
// subtree.
func (s *DreamAdminStore) CycleInOrg(ctx context.Context, orgID uuid.UUID, cycleID uuid.UUID) (bool, error) {
	cycle, err := s.cycleRepo.GetByID(ctx, cycleID)
	if err != nil {
		return false, nil
	}
	projPath, err := s.projectNamespacePath(ctx, cycle.ProjectID)
	if err != nil {
		return false, nil
	}
	orgPath, err := s.orgNamespacePath(ctx, orgID)
	if err != nil {
		return false, err
	}
	if projPath == orgPath {
		return true, nil
	}
	prefix := orgPath + "/"
	return len(projPath) > len(prefix) && projPath[:len(prefix)] == prefix, nil
}

// CycleInNamespacePrefix returns true iff the cycle's project namespace
// path is equal to or descended from the given prefix. Used by self-tier
// abandon/rollback to gate on caller ownership.
func (s *DreamAdminStore) CycleInNamespacePrefix(ctx context.Context, prefix string, cycleID uuid.UUID) (bool, error) {
	cycle, err := s.cycleRepo.GetByID(ctx, cycleID)
	if err != nil {
		return false, nil
	}
	projPath, err := s.projectNamespacePath(ctx, cycle.ProjectID)
	if err != nil {
		return false, nil
	}
	if projPath == prefix {
		return true, nil
	}
	p := prefix + "/"
	return len(projPath) > len(p) && projPath[:len(p)] == p, nil
}

func (s *DreamAdminStore) isEnabled(ctx context.Context) bool {
	setting, err := s.settingsRepo.Get(ctx, service.SettingDreamingEnabled, "global")
	if err != nil {
		// No global override row exists: fall through to the built-in default
		// so the status display matches what the scheduler's cascade resolver
		// (ResolveBool at global scope) actually sees. Otherwise a true default
		// would run dreaming while this banner reported it disabled.
		def, _ := service.GetDefault(service.SettingDreamingEnabled)
		return def == "true"
	}
	var val string
	if err := json.Unmarshal(setting.Value, &val); err != nil {
		return false
	}
	return val == "true"
}
