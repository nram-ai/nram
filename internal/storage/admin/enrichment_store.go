package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

// SelfQueueStatus returns the queue items whose memory.namespace_id is
// descended from the given user namespace. Counts are also scoped to the
// caller — they reflect only the caller's queue, not the system-wide
// queue. Used by the tier-A /v1/me/enrichment handler.
func (s *EnrichmentAdminStore) SelfQueueStatus(ctx context.Context, userNamespaceID uuid.UUID) (*api.EnrichmentQueueStatus, error) {
	// Resolve caller's namespace path so we can filter by prefix.
	var callerPath string
	row := s.db.QueryRow(ctx, "SELECT path FROM namespaces WHERE id = ?", userNamespaceID.String())
	if s.db.Backend() == storage.BackendPostgres {
		row = s.db.QueryRow(ctx, "SELECT path FROM namespaces WHERE id = $1", userNamespaceID.String())
	}
	if err := row.Scan(&callerPath); err != nil {
		return nil, fmt.Errorf("self queue caller namespace: %w", err)
	}

	prefixPattern := callerPath + "/%"
	exactPath := callerPath

	// Counts.
	var counts api.EnrichmentQueueCounts
	for _, st := range []struct {
		status string
		dest   *int
	}{
		{"pending", &counts.Pending},
		{"processing", &counts.Processing},
		{"completed", &counts.Completed},
		{"failed", &counts.Failed},
	} {
		var q string
		if s.db.Backend() == storage.BackendPostgres {
			q = `SELECT COUNT(*) FROM enrichment_queue eq
				JOIN memories m ON eq.memory_id = m.id
				JOIN namespaces n ON m.namespace_id = n.id
				WHERE eq.status = $1 AND (n.path = $2 OR n.path LIKE $3)`
		} else {
			q = `SELECT COUNT(*) FROM enrichment_queue eq
				JOIN memories m ON eq.memory_id = m.id
				JOIN namespaces n ON m.namespace_id = n.id
				WHERE eq.status = ? AND (n.path = ? OR n.path LIKE ?)`
		}
		row := s.db.QueryRow(ctx, q, st.status, exactPath, prefixPattern)
		_ = row.Scan(st.dest)
	}

	// Recent items in caller's scope.
	var itemsQ string
	if s.db.Backend() == storage.BackendPostgres {
		itemsQ = `SELECT eq.id, eq.memory_id, eq.status, eq.attempts, eq.last_error, eq.created_at
			FROM enrichment_queue eq
			JOIN memories m ON eq.memory_id = m.id
			JOIN namespaces n ON m.namespace_id = n.id
			WHERE n.path = $1 OR n.path LIKE $2
			ORDER BY eq.created_at DESC LIMIT 50`
	} else {
		itemsQ = `SELECT eq.id, eq.memory_id, eq.status, eq.attempts, eq.last_error, eq.created_at
			FROM enrichment_queue eq
			JOIN memories m ON eq.memory_id = m.id
			JOIN namespaces n ON m.namespace_id = n.id
			WHERE n.path = ? OR n.path LIKE ?
			ORDER BY eq.created_at DESC LIMIT 50`
	}
	rows, err := s.db.Query(ctx, itemsQ, exactPath, prefixPattern)
	if err != nil {
		return nil, fmt.Errorf("self queue items: %w", err)
	}
	defer rows.Close()

	queueItems := []api.EnrichmentQueueItem{}
	for rows.Next() {
		var (
			idStr, memIDStr, status, createdAtStr string
			attempts                              int
			lastErr                               *string
		)
		if err := rows.Scan(&idStr, &memIDStr, &status, &attempts, &lastErr, &createdAtStr); err != nil {
			return nil, fmt.Errorf("self queue scan: %w", err)
		}
		id, _ := uuid.Parse(idStr)
		memID, _ := uuid.Parse(memIDStr)
		ts, _ := parseQueueTime(createdAtStr)
		errStr := ""
		if lastErr != nil {
			errStr = *lastErr
		}
		queueItems = append(queueItems, api.EnrichmentQueueItem{
			ID:        id,
			MemoryID:  memID,
			Status:    status,
			Attempts:  attempts,
			LastError: errStr,
			CreatedAt: ts,
		})
	}

	paused, _ := s.IsPaused(ctx)

	return &api.EnrichmentQueueStatus{
		Counts: counts,
		Items:  queueItems,
		Paused: paused,
	}, nil
}

func parseQueueTime(s string) (t time.Time, err error) {
	for _, layout := range []string{
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		time.RFC3339Nano,
		time.RFC3339,
	} {
		if pt, perr := time.Parse(layout, s); perr == nil {
			return pt, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp: %s", s)
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
