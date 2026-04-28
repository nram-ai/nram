package dreaming

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// RetentionSweeper compresses old dream logs into summaries, removes
// detailed log entries past the retention window, and hard-deletes
// soft-deleted memories whose retention window has also elapsed.
type RetentionSweeper struct {
	logRepo       *storage.DreamLogRepo
	cycleRepo     *storage.DreamCycleRepo
	memoryDeleter MemoryHardDeleter
	settings      SettingsResolver
}

// NewRetentionSweeper creates a new RetentionSweeper. memoryDeleter may be
// nil, in which case the soft-delete hard-purge pass is skipped and only
// dream-log compression runs.
func NewRetentionSweeper(
	logRepo *storage.DreamLogRepo,
	cycleRepo *storage.DreamCycleRepo,
	memoryDeleter MemoryHardDeleter,
	settings SettingsResolver,
) *RetentionSweeper {
	return &RetentionSweeper{
		logRepo:       logRepo,
		cycleRepo:     cycleRepo,
		memoryDeleter: memoryDeleter,
		settings:      settings,
	}
}

// Sweep processes all projects, compressing logs past the retention window.
// It finds cycles with logs older than the retention period, creates summaries,
// and deletes the detailed logs.
func (s *RetentionSweeper) Sweep(ctx context.Context) error {
	retentionDays, err := s.settings.ResolveInt(ctx, service.SettingDreamLogRetention, "global")
	if err != nil || retentionDays <= 0 {
		retentionDays = 30
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)

	// Find all completed/failed cycles with logs older than the cutoff.
	// We process up to 200 cycles per sweep to bound per-sweep work.
	cycles, err := s.cycleRepo.ListRecent(ctx, 200)
	if err != nil {
		return err
	}

	compressed := 0
	for _, cycle := range cycles {
		if cycle.CompletedAt == nil {
			continue
		}
		if cycle.CompletedAt.After(cutoff) {
			continue
		}

		// Check if this cycle still has detailed logs.
		count, err := s.logRepo.CountByCycle(ctx, cycle.ID)
		if err != nil || count == 0 {
			continue
		}

		// Build summary from the logs.
		logs, err := s.logRepo.ListByCycle(ctx, cycle.ID)
		if err != nil {
			slog.Warn("dreaming: retention sweep failed to list logs",
				"cycle", cycle.ID, "err", err)
			continue
		}

		summary := buildLogSummary(logs)
		summaryJSON, _ := json.Marshal(summary)

		// Create summary record.
		if err := s.logRepo.CreateSummary(ctx, &model.DreamLogSummary{
			CycleID:   cycle.ID,
			ProjectID: cycle.ProjectID,
			Summary:   summaryJSON,
		}); err != nil {
			slog.Warn("dreaming: retention sweep failed to create summary",
				"cycle", cycle.ID, "err", err)
			continue
		}

		// Delete detailed logs.
		if err := s.logRepo.DeleteByCycle(ctx, cycle.ID); err != nil {
			slog.Warn("dreaming: retention sweep failed to delete logs",
				"cycle", cycle.ID, "err", err)
			continue
		}

		compressed++
	}

	if compressed > 0 {
		slog.Info("dreaming: retention sweep compressed cycles", "count", compressed)
	}

	// Hard-delete soft-deleted memories past their own retention window.
	// FK ON DELETE actions reap child rows (CASCADE for vectors / lineage /
	// enrichment_queue, SET NULL for token_usage / relationships); any
	// still-attached VectorStore saw the in-memory node dropped at
	// soft-delete time. Bounded per sweep so a single call cannot stall on
	// a backlog.
	if s.memoryDeleter != nil {
		memRetentionDays, err := s.settings.ResolveInt(ctx, service.SettingMemorySoftDeleteRetentionDays, "global")
		if err != nil || memRetentionDays <= 0 {
			memRetentionDays = 30
		}
		memCutoff := time.Now().UTC().AddDate(0, 0, -memRetentionDays)
		if deleted, derr := s.memoryDeleter.HardDeleteSoftDeletedBefore(ctx, memCutoff, 1000); derr != nil {
			slog.Warn("dreaming: retention sweep hard-delete memories failed", "err", derr)
		} else if deleted > 0 {
			slog.Info("dreaming: retention sweep hard-deleted memories",
				"count", deleted, "retention_days", memRetentionDays)
		}
	}

	return nil
}

type logSummaryData struct {
	TotalOperations int            `json:"total_operations"`
	ByPhase         map[string]int `json:"by_phase"`
	ByOperation     map[string]int `json:"by_operation"`
	ByTargetType    map[string]int `json:"by_target_type"`
}

func buildLogSummary(logs []model.DreamLog) logSummaryData {
	summary := logSummaryData{
		TotalOperations: len(logs),
		ByPhase:         make(map[string]int),
		ByOperation:     make(map[string]int),
		ByTargetType:    make(map[string]int),
	}

	for _, log := range logs {
		summary.ByPhase[log.Phase]++
		summary.ByOperation[log.Operation]++
		summary.ByTargetType[log.TargetType]++
	}

	return summary
}
