package admin

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// DreamAdminStore provides admin-level access to dream cycle data.
// It implements api.DreamAdminStore.
type DreamAdminStore struct {
	cycleRepo    *storage.DreamCycleRepo
	logRepo      *storage.DreamLogRepo
	dirtyRepo    *storage.DreamDirtyRepo
	settingsRepo *storage.SettingsRepo
}

// NewDreamAdminStore creates a new DreamAdminStore.
func NewDreamAdminStore(
	cycleRepo *storage.DreamCycleRepo,
	logRepo *storage.DreamLogRepo,
	dirtyRepo *storage.DreamDirtyRepo,
	settingsRepo *storage.SettingsRepo,
) *DreamAdminStore {
	return &DreamAdminStore{
		cycleRepo:    cycleRepo,
		logRepo:      logRepo,
		dirtyRepo:    dirtyRepo,
		settingsRepo: settingsRepo,
	}
}

// Status returns the system-wide dream status.
func (s *DreamAdminStore) Status(ctx context.Context) (*api.DreamStatusResponse, error) {
	dirtyCount, _ := s.dirtyRepo.CountDirty(ctx)
	cycles, _ := s.cycleRepo.ListRecent(ctx, 10)
	if cycles == nil {
		cycles = []model.DreamCycle{}
	}

	enabled := s.isEnabled(ctx)

	return &api.DreamStatusResponse{
		Enabled:      enabled,
		DirtyCount:   dirtyCount,
		RecentCycles: cycles,
	}, nil
}

// ProjectStatus returns the dream status for a specific project.
func (s *DreamAdminStore) ProjectStatus(ctx context.Context, projectID uuid.UUID) (*api.DreamProjectStatusResponse, error) {
	dirty, _ := s.dirtyRepo.IsDirty(ctx, projectID)
	cycles, _ := s.cycleRepo.ListByProject(ctx, projectID, 10)
	if cycles == nil {
		cycles = []model.DreamCycle{}
	}

	var lastDream *model.DreamCycle
	if len(cycles) > 0 {
		lastDream = &cycles[0]
	}

	return &api.DreamProjectStatusResponse{
		Enabled:   s.isProjectEnabled(ctx, projectID),
		Dirty:     dirty,
		LastDream:  lastDream,
		Cycles:    cycles,
	}, nil
}

// ListCycles returns dream cycles, optionally filtered by project.
func (s *DreamAdminStore) ListCycles(ctx context.Context, projectID *uuid.UUID, limit int) ([]model.DreamCycle, error) {
	if projectID != nil {
		return s.cycleRepo.ListByProject(ctx, *projectID, limit)
	}
	return s.cycleRepo.ListRecent(ctx, limit)
}

// GetCycleLogs returns the log entries for a specific cycle.
func (s *DreamAdminStore) GetCycleLogs(ctx context.Context, cycleID uuid.UUID) ([]model.DreamLog, error) {
	return s.logRepo.ListByCycle(ctx, cycleID)
}

// GetCycle returns a specific dream cycle by ID.
func (s *DreamAdminStore) GetCycle(ctx context.Context, cycleID uuid.UUID) (*model.DreamCycle, error) {
	return s.cycleRepo.GetByID(ctx, cycleID)
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

// SetProjectEnabled sets the dreaming enabled state for a specific project.
func (s *DreamAdminStore) SetProjectEnabled(ctx context.Context, projectID uuid.UUID, enabled bool) error {
	val := "false"
	if enabled {
		val = "true"
	}
	value, _ := json.Marshal(val)
	return s.settingsRepo.Set(ctx, &model.Setting{
		Key:   service.SettingDreamProjectEnabled,
		Value: json.RawMessage(value),
		Scope: "project:" + projectID.String(),
	})
}

func (s *DreamAdminStore) isProjectEnabled(ctx context.Context, projectID uuid.UUID) bool {
	setting, err := s.settingsRepo.Get(ctx, service.SettingDreamProjectEnabled, "project:"+projectID.String())
	if err != nil {
		return true // default: enabled (opt-out)
	}
	var val string
	if err := json.Unmarshal(setting.Value, &val); err != nil {
		return true
	}
	return val == "true" || val == "1"
}

func (s *DreamAdminStore) isEnabled(ctx context.Context) bool {
	setting, err := s.settingsRepo.Get(ctx, service.SettingDreamingEnabled, "global")
	if err != nil {
		return false
	}
	var val string
	if err := json.Unmarshal(setting.Value, &val); err != nil {
		return false
	}
	return val == "true"
}
