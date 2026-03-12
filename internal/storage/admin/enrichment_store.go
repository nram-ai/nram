package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// EnrichmentAdminStore implements api.EnrichmentAdminStore by wrapping
// EnrichmentQueueRepo and SettingsRepo for pause state.
type EnrichmentAdminStore struct {
	queueRepo    *storage.EnrichmentQueueRepo
	settingsRepo *storage.SettingsRepo
	db           storage.DB
}

// NewEnrichmentAdminStore creates a new EnrichmentAdminStore.
func NewEnrichmentAdminStore(
	queueRepo *storage.EnrichmentQueueRepo,
	settingsRepo *storage.SettingsRepo,
	db storage.DB,
) *EnrichmentAdminStore {
	return &EnrichmentAdminStore{
		queueRepo:    queueRepo,
		settingsRepo: settingsRepo,
		db:           db,
	}
}

func (s *EnrichmentAdminStore) QueueStatus(ctx context.Context) (*api.EnrichmentQueueStatus, error) {
	stats, err := s.queueRepo.CountByStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("queue status counts: %w", err)
	}

	// Get completed count.
	var completed int
	row := s.db.QueryRow(ctx, "SELECT COUNT(*) FROM enrichment_queue WHERE status = 'completed'")
	row.Scan(&completed)

	// Get recent items.
	items, err := s.queueRepo.ListRecent(ctx, 50)
	if err != nil {
		return nil, fmt.Errorf("queue status items: %w", err)
	}

	queueItems := []api.EnrichmentQueueItem{}
	for _, item := range items {
		lastErr := ""
		if item.LastError != nil {
			lastErr = string(item.LastError)
		}
		queueItems = append(queueItems, api.EnrichmentQueueItem{
			ID:        item.ID,
			MemoryID:  item.MemoryID,
			Status:    item.Status,
			Attempts:  item.Attempts,
			LastError: lastErr,
			CreatedAt: item.CreatedAt,
		})
	}

	paused, _ := s.IsPaused(ctx)

	return &api.EnrichmentQueueStatus{
		Counts: api.EnrichmentQueueCounts{
			Pending:    stats.Pending,
			Processing: stats.Processing,
			Completed:  completed,
			Failed:     stats.Failed,
		},
		Items:  queueItems,
		Paused: paused,
	}, nil
}

func (s *EnrichmentAdminStore) RetryFailed(ctx context.Context, ids []uuid.UUID) (int, error) {
	if len(ids) == 0 {
		return s.queueRepo.RetryAllFailed(ctx)
	}

	count := 0
	for _, id := range ids {
		if err := s.queueRepo.Retry(ctx, id); err == nil {
			count++
		}
	}
	return count, nil
}

func (s *EnrichmentAdminStore) SetPaused(ctx context.Context, paused bool) error {
	value, _ := json.Marshal(paused)
	setting := &model.Setting{
		Key:   "enrichment.paused",
		Value: json.RawMessage(value),
		Scope: "global",
	}
	return s.settingsRepo.Set(ctx, setting)
}

func (s *EnrichmentAdminStore) IsPaused(ctx context.Context) (bool, error) {
	setting, err := s.settingsRepo.Get(ctx, "enrichment.paused", "global")
	if err != nil {
		return false, nil // not set = not paused
	}

	var paused bool
	if err := json.Unmarshal(setting.Value, &paused); err != nil {
		return false, nil
	}
	return paused, nil
}
