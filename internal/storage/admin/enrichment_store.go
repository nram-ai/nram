package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
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

// hydrateQueueItem builds the api-layer EnrichmentQueueItem from a queue
// row. projectName is empty on admin-tier callers; the UI falls through
// to project_id. augmentedQueries / augmentedEmbeddingAt are joined from
// the memories row so the enrichment-monitor "Augmentation" panel renders
// the persisted state without a second fetch.
func (s *EnrichmentAdminStore) hydrateQueueItem(item model.EnrichmentJob, projectID *uuid.UUID, projectName string, augmentedQueries []string, augmentedEmbeddingAt *time.Time, staleThresholdMs int64, now time.Time) api.EnrichmentQueueItem {
	lastErr := ""
	if item.LastError != nil {
		lastErr = string(item.LastError)
	}
	steps := []string{}
	if len(item.StepsCompleted) > 0 {
		var parsed []string
		if err := json.Unmarshal(item.StepsCompleted, &parsed); err == nil && parsed != nil {
			// Filter internal sentinels (e.g. paraphrase-guard backfill
			// marker) so the UI shows only real model.Step* constants.
			steps = make([]string, 0, len(parsed))
			for _, s := range parsed {
				if s == model.JobMarkerOnlyParaphraseGuard {
					continue
				}
				steps = append(steps, s)
			}
		}
	}
	out := api.EnrichmentQueueItem{
		ID:                     item.ID,
		MemoryID:               item.MemoryID,
		ProjectID:              projectID,
		ProjectName:            projectName,
		Status:                 item.Status,
		Attempts:               item.Attempts,
		MaxAttempts:            item.MaxAttempts,
		LastError:              lastErr,
		CreatedAt:              item.CreatedAt,
		ClaimedBy:              item.ClaimedBy,
		LastRequeueReason:      item.LastRequeueReason,
		StepsCompleted:         steps,
		QueryAugmentSkipReason: item.QueryAugmentSkipReason,
		AugmentedQueries:       augmentedQueries,
		AugmentedEmbeddingAt:   augmentedEmbeddingAt,
	}
	if item.Status == model.EnrichmentStatusProcessing && item.ClaimedAt != nil {
		out.ClaimedAt = item.ClaimedAt
		ageMs := max(now.Sub(*item.ClaimedAt).Milliseconds(), 0)
		out.ClaimedAtAgeMs = &ageMs
		// Half-threshold is the early-warning point, same intent as
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

// queueItemSelectColumns is the projection shared between SelfQueueStatus
// (with p.name), OrgQueueStatus (without), and QueueStatus (without). When
// withName is true the SELECT adds `p.name` so self-tier callers see
// project names; org and system paths leave it off and surface project_id
// only; org-tier views must not leak other users' project names to an
// org_owner, matching the system-tier privacy posture. The trailing
// m.augmented_queries and m.augmented_embedding_at columns are always
// included so the enrichment-monitor "Augmentation" panel can render the
// persisted badge straight from the queue payload.
func queueItemSelectColumns(withName bool) string {
	cols := `eq.id, eq.memory_id, eq.status, eq.attempts, eq.max_attempts, eq.last_error, eq.created_at,
		eq.claimed_by, eq.claimed_at, eq.last_requeue_reason, eq.steps_completed, eq.query_augment_skip_reason, p.id`
	if withName {
		cols += `, p.name`
	}
	cols += `, m.augmented_queries, m.augmented_embedding_at`
	return cols
}

// scanQueueItem reads one row and builds an EnrichmentQueueItem. When
// withName is true the scan reads p.name before the trailing memory
// augmentation columns; otherwise the row's project section stops at p.id.
// augmented_queries is stored on memories as a JSON-encoded array string
// (see internal/storage/memory_repo.go); we tolerate "null" / empty by
// emitting a nil slice so the UI's truthy check renders the not-augmented
// state. augmented_embedding_at is RFC3339 in both backends.
func (s *EnrichmentAdminStore) scanQueueItem(rows *sql.Rows, withName bool, threshold int64, now time.Time) (api.EnrichmentQueueItem, error) {
	var (
		idStr, memIDStr, status, createdAtStr     string
		attempts, maxAttempts                     int
		lastErr, claimedBy, claimedAtStr, requeue *string
		stepsCompletedStr                         string
		queryAugmentSkipReason                    *string
		projectIDStr                              *string
		projectName                               *string
		augmentedQueriesStr                       sql.NullString
		augmentedEmbeddingAtStr                   sql.NullString
	)
	dest := []any{&idStr, &memIDStr, &status, &attempts, &maxAttempts, &lastErr, &createdAtStr,
		&claimedBy, &claimedAtStr, &requeue, &stepsCompletedStr, &queryAugmentSkipReason, &projectIDStr}
	if withName {
		dest = append(dest, &projectName)
	}
	dest = append(dest, &augmentedQueriesStr, &augmentedEmbeddingAtStr)
	if err := rows.Scan(dest...); err != nil {
		return api.EnrichmentQueueItem{}, err
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
	var pid *uuid.UUID
	if projectIDStr != nil && *projectIDStr != "" {
		if pu, perr := uuid.Parse(*projectIDStr); perr == nil {
			pid = &pu
		}
	}
	pname := ""
	if projectName != nil {
		pname = *projectName
	}
	var stepsCompletedRaw json.RawMessage
	if stepsCompletedStr != "" {
		stepsCompletedRaw = json.RawMessage(stepsCompletedStr)
	}

	var augmentedQueries []string
	if augmentedQueriesStr.Valid && augmentedQueriesStr.String != "" && augmentedQueriesStr.String != "null" {
		// Parse failures are non-fatal: the runtime contract that wrote the
		// column already validated it; if a future drift produces unparsable
		// JSON here, returning nil is the correct fail-soft (UI shows
		// "Raw embed" instead of crashing the entire queue payload).
		_ = json.Unmarshal([]byte(augmentedQueriesStr.String), &augmentedQueries)
	}
	var augmentedEmbeddingAt *time.Time
	if augmentedEmbeddingAtStr.Valid && augmentedEmbeddingAtStr.String != "" {
		if t, perr := time.Parse(time.RFC3339, augmentedEmbeddingAtStr.String); perr == nil {
			augmentedEmbeddingAt = &t
		}
	}

	return s.hydrateQueueItem(model.EnrichmentJob{
		ID:                     id,
		MemoryID:               memID,
		Status:                 status,
		Attempts:               attempts,
		MaxAttempts:            maxAttempts,
		LastError:              lastErrJSON,
		CreatedAt:              ts,
		ClaimedBy:              claimedBy,
		ClaimedAt:              claimedAt,
		LastRequeueReason:      requeue,
		StepsCompleted:         stepsCompletedRaw,
		QueryAugmentSkipReason: queryAugmentSkipReason,
	}, pid, pname, augmentedQueries, augmentedEmbeddingAt, threshold, now), nil
}

// placeholderFn returns a generator that emits successive SQL bind
// placeholders for the active backend: "$1", "$2", … for Postgres or "?" for
// SQLite. Each call advances the counter, so callers must invoke it in the
// same order the corresponding args are appended.
func placeholderFn(pg bool) func() string {
	idx := 0
	return func() string {
		idx++
		if pg {
			return "$" + strconv.Itoa(idx)
		}
		return "?"
	}
}

// queueOrderClause renders the ORDER BY tail for a queue list query from
// pre-normalized params (see api.QueueListParams.Normalize). The Sort column
// and Dir are whitelisted literals, never bound parameters. A deterministic
// (eq.created_at DESC, eq.id) tiebreaker is always appended so rows that share
// the primary sort key keep a stable order across refetches; without it,
// batch-enqueued rows with identical created_at shuffle between polls and the
// UI table visibly jumps.
func queueOrderClause(params api.QueueListParams) string {
	dir := "DESC"
	if params.Dir == "asc" {
		dir = "ASC"
	}
	switch params.Sort {
	case "status":
		return " ORDER BY eq.status " + dir + ", eq.created_at DESC, eq.id ASC"
	case "attempts":
		return " ORDER BY eq.attempts " + dir + ", eq.created_at DESC, eq.id ASC"
	default: // created_at
		return " ORDER BY eq.created_at " + dir + ", eq.id ASC"
	}
}

// buildQueueListItemsQuery assembles the items query shared by the self, org,
// and system-wide queue-list methods. fromJoin is the FROM ... JOIN block
// (immediately following the column list); where is the scope predicate
// already built with leading placeholders from ph (empty for the system-wide
// view, which has no namespace scope); args holds the values bound to those
// placeholders. It appends the optional status filter (choosing WHERE vs AND
// based on whether a scope predicate exists), then the deterministic ORDER BY
// and LIMIT/OFFSET, returning the finished query and complete args. ph must be
// the same generator used to build where so placeholder numbering stays
// contiguous. params must already be Normalized by the caller.
func buildQueueListItemsQuery(cols, fromJoin, where string, args []any, ph func() string, params api.QueueListParams) (string, []any) {
	if params.Status != "" {
		if where == "" {
			where = "eq.status = " + ph()
		} else {
			where += " AND eq.status = " + ph()
		}
		args = append(args, params.Status)
	}
	q := `SELECT ` + cols + fromJoin
	if where != "" {
		q += ` WHERE ` + where
	}
	q += queueOrderClause(params) + ` LIMIT ` + ph()
	args = append(args, params.Limit)
	q += ` OFFSET ` + ph()
	args = append(args, params.Offset)
	return q, args
}

// SelfQueueStatus returns the queue items whose memory.namespace_id is
// descended from the given user namespace. Counts are also scoped to the
// caller. Used by /v1/me/enrichment.
func (s *EnrichmentAdminStore) SelfQueueStatus(ctx context.Context, userNamespaceID uuid.UUID, params api.QueueListParams) (*api.EnrichmentQueueStatus, error) {
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

	var counts api.EnrichmentQueueCounts
	for _, st := range []struct {
		status string
		dest   *int
	}{
		{model.EnrichmentStatusPending, &counts.Pending},
		{model.EnrichmentStatusProcessing, &counts.Processing},
		{model.EnrichmentStatusCompleted, &counts.Completed},
		{model.EnrichmentStatusFailed, &counts.Failed},
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

	params = params.Normalize()
	ph := placeholderFn(s.db.Backend() == storage.BackendPostgres)
	where := "n.path = " + ph() + " OR n.path LIKE " + ph()
	itemsQ, args := buildQueueListItemsQuery(
		queueItemSelectColumns(true),
		`
		FROM enrichment_queue eq
		JOIN memories m ON eq.memory_id = m.id
		JOIN namespaces n ON m.namespace_id = n.id
		LEFT JOIN projects p ON p.namespace_id = m.namespace_id`,
		where, []any{exactPath, prefixPattern}, ph, params,
	)

	rows, err := s.db.Query(ctx, itemsQ, args...)
	if err != nil {
		return nil, fmt.Errorf("self queue items: %w", err)
	}
	defer func() { _ = rows.Close() }()

	threshold := s.staleThresholdMs(ctx)
	now := time.Now().UTC()
	queueItems := []api.EnrichmentQueueItem{}
	for rows.Next() {
		item, err := s.scanQueueItem(rows, true, threshold, now)
		if err != nil {
			return nil, fmt.Errorf("self queue scan: %w", err)
		}
		queueItems = append(queueItems, item)
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

// QueueStatus returns the system-wide queue. Items carry project_id (no
// project_name) so cross-tenant admins see UUIDs only, matching the
// privacy posture for system-tier dreaming cycles.
func (s *EnrichmentAdminStore) QueueStatus(ctx context.Context, params api.QueueListParams) (*api.EnrichmentQueueStatus, error) {
	stats, err := s.queueRepo.CountByStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("queue status counts: %w", err)
	}

	params = params.Normalize()
	ph := placeholderFn(s.db.Backend() == storage.BackendPostgres)
	itemsQ, args := buildQueueListItemsQuery(
		queueItemSelectColumns(false),
		`
		FROM enrichment_queue eq
		JOIN memories m ON eq.memory_id = m.id
		LEFT JOIN projects p ON p.namespace_id = m.namespace_id`,
		"", nil, ph, params,
	)

	rows, err := s.db.Query(ctx, itemsQ, args...)
	if err != nil {
		return nil, fmt.Errorf("queue status items: %w", err)
	}
	defer func() { _ = rows.Close() }()

	threshold := s.staleThresholdMs(ctx)
	now := time.Now().UTC()
	queueItems := []api.EnrichmentQueueItem{}
	for rows.Next() {
		item, err := s.scanQueueItem(rows, false, threshold, now)
		if err != nil {
			return nil, fmt.Errorf("queue status scan: %w", err)
		}
		queueItems = append(queueItems, item)
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

// orgNamespacePath returns the org's root namespace path. Used as the LIKE
// prefix for org-scoped queue / cycle queries.
func (s *EnrichmentAdminStore) orgNamespacePath(ctx context.Context, orgID uuid.UUID) (string, error) {
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

// OrgQueueStatus returns the queue items whose memory.namespace_id is
// descended from the given org's root namespace. Counts are scoped to the
// same subtree. Items carry project_id (no project_name) so an org_owner
// sees UUIDs only for projects owned by other users in the org, matching
// the privacy posture for system-tier views. Used by
// /v1/orgs/{orgId}/enrichment.
func (s *EnrichmentAdminStore) OrgQueueStatus(ctx context.Context, orgID uuid.UUID, params api.QueueListParams) (*api.EnrichmentQueueStatus, error) {
	orgPath, err := s.orgNamespacePath(ctx, orgID)
	if err != nil {
		return nil, err
	}
	prefixPattern := orgPath + "/%"
	exactPath := orgPath

	var counts api.EnrichmentQueueCounts
	for _, st := range []struct {
		status string
		dest   *int
	}{
		{model.EnrichmentStatusPending, &counts.Pending},
		{model.EnrichmentStatusProcessing, &counts.Processing},
		{model.EnrichmentStatusCompleted, &counts.Completed},
		{model.EnrichmentStatusFailed, &counts.Failed},
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

	params = params.Normalize()
	ph := placeholderFn(s.db.Backend() == storage.BackendPostgres)
	where := "n.path = " + ph() + " OR n.path LIKE " + ph()
	itemsQ, args := buildQueueListItemsQuery(
		queueItemSelectColumns(false),
		`
		FROM enrichment_queue eq
		JOIN memories m ON eq.memory_id = m.id
		JOIN namespaces n ON m.namespace_id = n.id
		LEFT JOIN projects p ON p.namespace_id = m.namespace_id`,
		where, []any{exactPath, prefixPattern}, ph, params,
	)

	rows, err := s.db.Query(ctx, itemsQ, args...)
	if err != nil {
		return nil, fmt.Errorf("org queue items: %w", err)
	}
	defer func() { _ = rows.Close() }()

	threshold := s.staleThresholdMs(ctx)
	now := time.Now().UTC()
	queueItems := []api.EnrichmentQueueItem{}
	for rows.Next() {
		item, err := s.scanQueueItem(rows, false, threshold, now)
		if err != nil {
			return nil, fmt.Errorf("org queue scan: %w", err)
		}
		queueItems = append(queueItems, item)
	}

	// Pause is a global flag. Surface it so the org tab can render the
	// "workers paused" indicator, but the org-tier handler does not expose
	// the pause/resume control.
	paused, _ := s.IsPaused(ctx)

	return &api.EnrichmentQueueStatus{
		Counts: counts,
		Items:  queueItems,
		Paused: paused,
	}, nil
}

// jobInNamespacePrefix returns true iff the queue job's memory namespace
// matches or is descended from the given path.
func (s *EnrichmentAdminStore) jobInNamespacePrefix(ctx context.Context, prefix string, jobID uuid.UUID) bool {
	q := `SELECT 1 FROM enrichment_queue eq
		JOIN memories m ON eq.memory_id = m.id
		JOIN namespaces n ON m.namespace_id = n.id
		WHERE eq.id = ? AND (n.path = ? OR n.path LIKE ?)`
	if s.db.Backend() == storage.BackendPostgres {
		q = `SELECT 1 FROM enrichment_queue eq
			JOIN memories m ON eq.memory_id = m.id
			JOIN namespaces n ON m.namespace_id = n.id
			WHERE eq.id = $1 AND (n.path = $2 OR n.path LIKE $3)`
	}
	var one int
	row := s.db.QueryRow(ctx, q, jobID.String(), prefix, prefix+"/%")
	return row.Scan(&one) == nil
}

// retryFailedInNamespacePath retries failed enrichment jobs whose memory
// namespace matches or is descended from prefix. Empty ids retries every
// failed job in scope; non-empty ids are filtered against the prefix and
// silently skipped if out-of-scope. An empty prefix means global (no scope
// filter), the admin path that retries any job.
func (s *EnrichmentAdminStore) retryFailedInNamespacePath(ctx context.Context, prefix string, ids []uuid.UUID) (int, error) {
	if len(ids) == 0 {
		// Retry-all in scope: one set-based bulk reset rather than selecting
		// every failed id and looping a per-row Retry. That loop was an N+1
		// that, on SQLite, serializes tens of thousands of single-row UPDATEs
		// against the worker pool and effectively hangs the request for a large
		// failed backlog.
		return s.queueRepo.RetryAllFailedScoped(ctx, prefix)
	}

	// Explicit, user-selected ids: filter to the caller's namespace (skipped
	// when global) and retry per row. These sets are small and the per-row
	// Retry's redundant-pending drop handling is correct here.
	if prefix != "" {
		filtered := ids[:0:0]
		for _, id := range ids {
			if s.jobInNamespacePrefix(ctx, prefix, id) {
				filtered = append(filtered, id)
			}
		}
		ids = filtered
	}
	count := 0
	for _, id := range ids {
		if err := s.queueRepo.Retry(ctx, id); err == nil {
			count++
		}
	}
	return count, nil
}

// OrgRetryFailed retries failed enrichment jobs scoped to the given org.
func (s *EnrichmentAdminStore) OrgRetryFailed(ctx context.Context, orgID uuid.UUID, ids []uuid.UUID) (int, error) {
	orgPath, err := s.orgNamespacePath(ctx, orgID)
	if err != nil {
		return 0, err
	}
	return s.retryFailedInNamespacePath(ctx, orgPath, ids)
}

// SelfRetryFailed retries failed jobs whose memory namespace is descended
// from (or equal to) the caller's user namespace path.
func (s *EnrichmentAdminStore) SelfRetryFailed(ctx context.Context, userNamespacePath string, ids []uuid.UUID) (int, error) {
	return s.retryFailedInNamespacePath(ctx, userNamespacePath, ids)
}

// RetryFailed retries failed enrichment jobs globally (the system/admin path):
// empty ids retries every failed job, explicit ids retry those rows unscoped.
// Shares the one set-based mechanism with the org/self paths via an empty
// prefix rather than maintaining a parallel global variant.
func (s *EnrichmentAdminStore) RetryFailed(ctx context.Context, ids []uuid.UUID) (int, error) {
	return s.retryFailedInNamespacePath(ctx, "", ids)
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
