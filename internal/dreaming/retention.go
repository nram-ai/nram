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

// RetentionSweeper compresses old dream logs into summaries and removes
// detailed log entries past the retention window.
type RetentionSweeper struct {
	logRepo   *storage.DreamLogRepo
	cycleRepo *storage.DreamCycleRepo
	settings  SettingsResolver
}

// NewRetentionSweeper creates a new RetentionSweeper.
func NewRetentionSweeper(
	logRepo *storage.DreamLogRepo,
	cycleRepo *storage.DreamCycleRepo,
	settings SettingsResolver,
) *RetentionSweeper {
	return &RetentionSweeper{
		logRepo:   logRepo,
		cycleRepo: cycleRepo,
		settings:  settings,
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
