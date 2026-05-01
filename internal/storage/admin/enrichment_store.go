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

// EnrichmentAdminStore implements api.EnrichmentAdminStore by wrapping
// EnrichmentQueueRepo and SettingsRepo. settingsSvc resolves
// enrichment.stuck_threshold_seconds for the is_stale_diagnostic flag on
// each hydrated queue item.
type EnrichmentAdminStore struct {
	queueRepo    *storage.EnrichmentQueueRepo
	settingsRepo *storage.SettingsRepo
	settingsSvc  *service.SettingsService
	db           storage.DB
}

// NewEnrichmentAdminStore creates a new EnrichmentAdminStore.
func NewEnrichmentAdminStore(
	queueRepo *storage.EnrichmentQueueRepo,
	settingsRepo *storage.SettingsRepo,
	settingsSvc *service.SettingsService,
	db storage.DB,
) *EnrichmentAdminStore {
	return &EnrichmentAdminStore{
		queueRepo:    queueRepo,
		settingsRepo: settingsRepo,
		settingsSvc:  settingsSvc,
		db:           db,
	}
}

// hydrateQueueItem builds the api-layer EnrichmentQueueItem from the model
// row plus the stuck threshold (in ms). Centralized so the SelfQueueStatus
// and QueueStatus paths stay in sync as the UI grows.
func (s *EnrichmentAdminStore) hydrateQueueItem(item model.EnrichmentJob, staleThresholdMs int64, now time.Time) api.EnrichmentQueueItem {
	lastErr := ""
	if item.LastError != nil {
		lastErr = string(item.LastError)
	}
	out := api.EnrichmentQueueItem{
		ID:                item.ID,
		MemoryID:          item.MemoryID,
		Status:            item.Status,
		Attempts:          item.Attempts,
		MaxAttempts:       item.MaxAttempts,
		LastError:         lastErr,
		CreatedAt:         item.CreatedAt,
		ClaimedBy:         item.ClaimedBy,
		LastRequeueReason: item.LastRequeueReason,
	}
	if item.Status == model.EnrichmentStatusProcessing && item.ClaimedAt != nil {
		ageMs := now.Sub(*item.ClaimedAt).Milliseconds()
		if ageMs < 0 {
			ageMs = 0
		}
		out.ClaimedAtAgeMs = &ageMs
		// Half-threshold is the early-warning point — same intent as
		// dreaming's is_stale_diagnostic.
		if staleThresholdMs > 0 && ageMs > staleThresholdMs/2 {
			out.IsStaleDiagnostic = true
		}
	}
	return out
}

// staleThresholdMs returns enrichment.stuck_threshold_seconds in ms, with a
// safe fallback if the setting service is unwired.
func (s *EnrichmentAdminStore) staleThresholdMs(ctx context.Context) int64 {
	if s.settingsSvc == nil {
		return int64((30 * time.Minute).Milliseconds())
	}
	d := s.settingsSvc.ResolveDurationSecondsWithDefault(ctx,
		service.SettingEnrichmentStuckThreshold, "global")
	if d < time.Second {
		d = 30 * time.Minute
	}
	return d.Milliseconds()
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
		itemsQ = `SELECT eq.id, eq.memory_id, eq.status, eq.attempts, eq.max_attempts, eq.last_error, eq.created_at,
				eq.claimed_by, eq.claimed_at, eq.last_requeue_reason
			FROM enrichment_queue eq
			JOIN memories m ON eq.memory_id = m.id
			JOIN namespaces n ON m.namespace_id = n.id
			WHERE n.path = $1 OR n.path LIKE $2
			ORDER BY eq.created_at DESC LIMIT 50`
	} else {
		itemsQ = `SELECT eq.id, eq.memory_id, eq.status, eq.attempts, eq.max_attempts, eq.last_error, eq.created_at,
				eq.claimed_by, eq.claimed_at, eq.last_requeue_reason
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

	threshold := s.staleThresholdMs(ctx)
	now := time.Now().UTC()
	queueItems := []api.EnrichmentQueueItem{}
	for rows.Next() {
		var (
			idStr, memIDStr, status, createdAtStr      string
			attempts, maxAttempts                      int
			lastErr, claimedBy, claimedAtStr, requeue  *string
		)
		if err := rows.Scan(&idStr, &memIDStr, &status, &attempts, &maxAttempts, &lastErr, &createdAtStr,
			&claimedBy, &claimedAtStr, &requeue); err != nil {
			return nil, fmt.Errorf("self queue scan: %w", err)
		}
		id, _ := uuid.Parse(idStr)
		memID, _ := uuid.Parse(memIDStr)
		ts, _ := parseQueueTime(createdAtStr)
		var lastErrJSON json.RawMessage
		if lastErr != nil {
			lastErrJSON = json.RawMessage(*lastErr)
		}
		var claimedAt *time.Time
		if claimedAtStr != nil && *claimedAtStr != "" {
			if t, perr := parseQueueTime(*claimedAtStr); perr == nil {
				claimedAt = &t
			}
		}
		queueItems = append(queueItems, s.hydrateQueueItem(model.EnrichmentJob{
			ID:                id,
			MemoryID:          memID,
			Status:            status,
			Attempts:          attempts,
			MaxAttempts:       maxAttempts,
			LastError:         lastErrJSON,
			CreatedAt:         ts,
			ClaimedBy:         claimedBy,
			ClaimedAt:         claimedAt,
			LastRequeueReason: requeue,
		}, threshold, now))
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

	items, err := s.queueRepo.ListRecent(ctx, 50)
	if err != nil {
		return nil, fmt.Errorf("queue status items: %w", err)
	}

	threshold := s.staleThresholdMs(ctx)
	now := time.Now().UTC()
	queueItems := make([]api.EnrichmentQueueItem, 0, len(items))
	for _, item := range items {
		queueItems = append(queueItems, s.hydrateQueueItem(item, threshold, now))
	}

	paused, _ := s.IsPaused(ctx)

	return &api.EnrichmentQueueStatus{
		Counts: api.EnrichmentQueueCounts{
			Pending:    stats.Pending,
			Processing: stats.Processing,
			Completed:  stats.Completed,
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
