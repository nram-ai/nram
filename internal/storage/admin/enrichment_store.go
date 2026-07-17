package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// enrichmentPhaseOperations are the token_usage operation names surfaced as
// per-phase metrics on a queue item, in canonical pipeline order (which also
// drives the rendered ordering in attachPhaseMetrics). Derived from the
// canonical provider list so the two never drift.
var enrichmentPhaseOperations = phaseOpStrings(provider.EnrichmentPhaseOperations())

func phaseOpStrings(ops []provider.Operation) []string {
	out := make([]string, len(ops))
	for i, op := range ops {
		out[i] = string(op)
	}
	return out
}

// phaseMetricLowerBoundSlack absorbs the second-level truncation of
// token_usage.created_at (stored as RFC3339 to the second) when comparing
// against a job's claimed/created timestamp, so a row recorded in the same
// second as the claim is not falsely excluded.
const phaseMetricLowerBoundSlack = 5 * time.Second

// EnrichmentAdminStore implements api.EnrichmentAdminStore by wrapping
// EnrichmentQueueRepo and SettingsRepo. settingsSvc resolves
// enrichment.stuck_threshold_seconds for the is_stale_diagnostic flag on
// each hydrated queue item.
type EnrichmentAdminStore struct {
	queueRepo    *storage.EnrichmentQueueRepo
	settingsRepo *storage.SettingsRepo
	settingsSvc  *service.SettingsService
	db           storage.DB
	tokenRepo    *storage.TokenUsageRepo
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
		tokenRepo:    storage.NewTokenUsageRepo(db),
	}
}

// attachPhaseMetrics hydrates each item's PhaseMetrics from the token_usage
// rows the provider middleware records per LLM/embedding call. Best-effort: on
// any failure it logs and leaves PhaseMetrics empty rather than failing the
// queue request. Batched into one query over the page's memory IDs.
//
// Rows are scoped to the item's run in one of two ways, tried in order:
//
//  1. Run-key match. The worker stamps model.EnrichmentRunKey(job.ID, attempts)
//     into token_usage.request_id for every phase of a run, so a row whose
//     request_id equals the item's key belongs to exactly that run. When any
//     such row exists this is authoritative: metrics come only from run-key
//     matches, which excludes prior runs/jobs of the same memory even when their
//     rows fall inside the old timestamp slack.
//  2. Timestamp fallback. For rows written before request_id stamping (nil
//     request_id) and for terminally-failed jobs whose attempts advanced past
//     the failed run's key, no run-key row matches; the item falls back to the
//     legacy window: the most recent row per operation, discarding rows older
//     than the job's claimed_at (or created_at), minus phaseMetricLowerBoundSlack.
//
// Either way it keeps the most recent row per operation (rows arrive created_at
// DESC), yields empty metrics for pending jobs (no rows yet), and surfaces the
// phases the displayed run actually executed.
func (s *EnrichmentAdminStore) attachPhaseMetrics(ctx context.Context, items []api.EnrichmentQueueItem) {
	if len(items) == 0 {
		return
	}

	memIDs := make([]uuid.UUID, 0, len(items))
	seenMem := make(map[uuid.UUID]bool, len(items))
	for _, it := range items {
		if !seenMem[it.MemoryID] {
			seenMem[it.MemoryID] = true
			memIDs = append(memIDs, it.MemoryID)
		}
	}

	rows, err := s.tokenRepo.ListByMemoryIDs(ctx, memIDs, enrichmentPhaseOperations)
	if err != nil {
		slog.Warn("enrichment: attach phase metrics", "err", err, "memories", len(memIDs))
		return
	}
	if len(rows) == 0 {
		return
	}

	byMem := make(map[uuid.UUID][]model.TokenUsage, len(memIDs))
	for _, r := range rows {
		if r.MemoryID == nil {
			continue
		}
		byMem[*r.MemoryID] = append(byMem[*r.MemoryID], r)
	}

	for i := range items {
		memRows := byMem[items[i].MemoryID]
		if len(memRows) == 0 {
			continue
		}

		// Prefer the per-run correlation key stamped into request_id; only fall
		// back to the timestamp window when no row carries this item's key.
		want := model.EnrichmentRunKey(items[i].ID, items[i].Attempts)
		haveRunKey := false
		for j := range memRows {
			if memRows[j].RequestID != nil && *memRows[j].RequestID == want {
				haveRunKey = true
				break
			}
		}

		lower := items[i].CreatedAt
		if items[i].ClaimedAt != nil {
			lower = *items[i].ClaimedAt
		}
		lower = lower.Add(-phaseMetricLowerBoundSlack)

		// keep reports whether a row belongs to the item's run under the active
		// scoping mode: exact request_id equality when a run-key row exists,
		// otherwise the timestamp lower bound.
		keep := func(r model.TokenUsage) bool {
			if haveRunKey {
				return r.RequestID != nil && *r.RequestID == want
			}
			return !r.CreatedAt.Before(lower)
		}

		// memRows are created_at DESC, so the first kept row matching an
		// operation is its most recent (the append+break below takes it).
		// Walking operations in canonical pipeline order yields metrics already
		// ordered and deduped, no order-map or sort.
		metrics := make([]api.EnrichmentPhaseMetric, 0, len(enrichmentPhaseOperations))
		for _, op := range enrichmentPhaseOperations {
			for _, r := range memRows {
				if r.Operation != op {
					continue
				}
				if !keep(r) {
					continue // wrong run (run-key mode) or predates the window
				}
				metrics = append(metrics, api.EnrichmentPhaseMetric{
					Operation:        r.Operation,
					Model:            r.Model,
					Provider:         r.Provider,
					PromptTokens:     r.TokensInput,
					CompletionTokens: r.TokensOutput,
					LatencyMs:        r.LatencyMs,
					Success:          r.Success,
					At:               r.CreatedAt,
				})
				break
			}
		}
		if len(metrics) == 0 {
			continue
		}
		items[i].PhaseMetrics = metrics
	}
}

// scanQueueItems drains a queue-list result set into items and attaches
// per-phase token metrics. It centralizes the scan loop and the
// attachPhaseMetrics post-pass so the three queue-status builders (self, org,
// system) cannot drift or forget the attach. threshold and now are resolved
// once here since every caller uses the same values.
func (s *EnrichmentAdminStore) scanQueueItems(ctx context.Context, rows *sql.Rows, withName bool) ([]api.EnrichmentQueueItem, error) {
	threshold := s.staleThresholdMs(ctx)
	now := time.Now().UTC()
	items := []api.EnrichmentQueueItem{}
	for rows.Next() {
		item, err := s.scanQueueItem(rows, withName, threshold, now)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	s.attachPhaseMetrics(ctx, items)
	return items, nil
}

// hydrateQueueItem builds the api-layer EnrichmentQueueItem from a queue
// row. projectName is empty on admin-tier callers; the UI falls through
// to project_id. augmentedQueries / augmentedEmbeddingAt are joined from
// the memories row so the enrichment-monitor "Augmentation" panel renders
// the persisted state without a second fetch.
func (s *EnrichmentAdminStore) hydrateQueueItem(item model.EnrichmentJob, projectID *uuid.UUID, projectName string, augmentedQueries []string, augmentedEmbeddingAt *time.Time, facetCount *int, staleThresholdMs int64, now time.Time) api.EnrichmentQueueItem {
	lastErr := ""
	if item.LastError != nil {
		lastErr = string(item.LastError)
	}
	steps := []string{}
	if len(item.StepsCompleted) > 0 {
		var parsed []string
		if err := json.Unmarshal(item.StepsCompleted, &parsed); err == nil && parsed != nil {
			// Filter internal sentinels (the paraphrase-guard and multi-vector
			// backfill markers) so the UI shows only real model.Step* constants.
			steps = make([]string, 0, len(parsed))
			for _, s := range parsed {
				if s == model.JobMarkerOnlyParaphraseGuard || s == model.JobMarkerOnlyMultiVector {
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
		FacetCount:             facetCount,
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
// m.augmented_queries, m.augmented_embedding_at, and m.facet_count columns are
// always included so the enrichment-monitor "Augmentation" panel and "Facets"
// line render the persisted state straight from the queue payload.
func queueItemSelectColumns(withName bool) string {
	cols := `eq.id, eq.memory_id, eq.status, eq.attempts, eq.max_attempts, eq.last_error, eq.created_at,
		eq.claimed_by, eq.claimed_at, eq.last_requeue_reason, eq.steps_completed, eq.query_augment_skip_reason, p.id`
	if withName {
		cols += `, p.name`
	}
	cols += `, m.augmented_queries, m.augmented_embedding_at, m.facet_count`
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
		facetCountVal                             sql.NullInt64
	)
	dest := []any{&idStr, &memIDStr, &status, &attempts, &maxAttempts, &lastErr, &createdAtStr,
		&claimedBy, &claimedAtStr, &requeue, &stepsCompletedStr, &queryAugmentSkipReason, &projectIDStr}
	if withName {
		dest = append(dest, &projectName)
	}
	dest = append(dest, &augmentedQueriesStr, &augmentedEmbeddingAtStr, &facetCountVal)
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
	var facetCount *int
	if facetCountVal.Valid {
		n := int(facetCountVal.Int64)
		facetCount = &n
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
	}, pid, pname, augmentedQueries, augmentedEmbeddingAt, facetCount, threshold, now), nil
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

// scopedNamespaceSubtree renders the scope predicate shared by the self and org
// queue-list and count queries: match enrichment_queue rows whose namespace is
// the given path or descended from it, via the denormalized
// enrichment_queue.namespace_id rather than a memories+namespaces join. A queue
// row cannot outlive its memory (memory_id is an ON DELETE CASCADE FK, migration
// 000023_memory_fk_cascade) and namespace_id is that memory's namespace, so this
// is equivalent to the old n.path join but lets idx_enrichment_queue_ns_status_created
// serve the filter, count grouping, and ordering. ph must be the generator used
// for the surrounding query so placeholder numbering stays contiguous; it is
// invoked twice and the caller must append exactPath then prefixPattern.
func scopedNamespaceSubtree(ph func() string) string {
	return "eq.namespace_id IN (SELECT id FROM namespaces WHERE path = " + ph() + " OR path LIKE " + ph() + ")"
}

// scopedCountByStatus returns per-status queue counts for a namespace subtree as
// a single GROUP BY over enrichment_queue.namespace_id, replacing four separate
// scoped COUNT(*) queries that each joined memories+namespaces and full-scanned
// the queue on a large failed backlog. Mirrors the system tier's
// EnrichmentQueueRepo.CountByStatus, scoped to the subtree.
func (s *EnrichmentAdminStore) scopedCountByStatus(ctx context.Context, exactPath, prefixPattern string) (api.EnrichmentQueueCounts, error) {
	var counts api.EnrichmentQueueCounts
	ph := placeholderFn(s.db.Backend() == storage.BackendPostgres)
	q := `SELECT eq.status, COUNT(*) FROM enrichment_queue eq WHERE ` +
		scopedNamespaceSubtree(ph) + ` GROUP BY eq.status`
	rows, err := s.db.Query(ctx, q, exactPath, prefixPattern)
	if err != nil {
		return counts, fmt.Errorf("scoped queue counts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return api.EnrichmentQueueCounts{}, fmt.Errorf("scoped queue counts scan: %w", err)
		}
		switch status {
		case model.EnrichmentStatusPending:
			counts.Pending = count
		case model.EnrichmentStatusProcessing:
			counts.Processing = count
		case model.EnrichmentStatusCompleted:
			counts.Completed = count
		case model.EnrichmentStatusFailed:
			counts.Failed = count
		}
	}
	return counts, rows.Err()
}

// scopedQueueStatus returns the counts + one page of items for a namespace
// subtree. It is the shared body of SelfQueueStatus and OrgQueueStatus, which
// differ only in how they resolve the path and whether project names are
// surfaced (self callers own every returned project; org/system see UUIDs only,
// matching the privacy posture). withProjectName threads through both
// queueItemSelectColumns and scanQueueItems.
func (s *EnrichmentAdminStore) scopedQueueStatus(ctx context.Context, exactPath, prefixPattern string, withProjectName bool, params api.QueueListParams) (*api.EnrichmentQueueStatus, error) {
	counts, err := s.scopedCountByStatus(ctx, exactPath, prefixPattern)
	if err != nil {
		return nil, err
	}

	params = params.Normalize()
	ph := placeholderFn(s.db.Backend() == storage.BackendPostgres)
	itemsQ, args := buildQueueListItemsQuery(
		queueItemSelectColumns(withProjectName),
		`
		FROM enrichment_queue eq
		JOIN memories m ON eq.memory_id = m.id
		LEFT JOIN projects p ON p.namespace_id = m.namespace_id`,
		scopedNamespaceSubtree(ph), []any{exactPath, prefixPattern}, ph, params,
	)

	rows, err := s.db.Query(ctx, itemsQ, args...)
	if err != nil {
		return nil, fmt.Errorf("scoped queue items: %w", err)
	}
	defer func() { _ = rows.Close() }()

	queueItems, err := s.scanQueueItems(ctx, rows, withProjectName)
	if err != nil {
		return nil, fmt.Errorf("scoped queue scan: %w", err)
	}

	paused, _ := s.IsPaused(ctx)
	return &api.EnrichmentQueueStatus{Counts: counts, Items: queueItems, Paused: paused}, nil
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
	// Self callers own every returned project, so project names are surfaced.
	return s.scopedQueueStatus(ctx, callerPath, callerPath+"/%", true, params)
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

	queueItems, err := s.scanQueueItems(ctx, rows, false)
	if err != nil {
		return nil, fmt.Errorf("queue status scan: %w", err)
	}

	paused, _ := s.IsPaused(ctx)
	health, _ := s.extractionHealth(ctx)

	return &api.EnrichmentQueueStatus{
		Counts: api.EnrichmentQueueCounts{
			Pending:    stats.Pending,
			Processing: stats.Processing,
			Completed:  stats.Completed,
			Failed:     stats.Failed,
		},
		Items:            queueItems,
		Paused:           paused,
		ExtractionHealth: health,
	}, nil
}

// extractionHealthWindow bounds the extraction-health tally to recent rows. The
// panel reports current ("rolling") extraction outcomes, so an all-time SUM over
// every last_error row is both unnecessary and, on a large failed backlog, a
// full table scan on every System-tab load. Bounding to a recent created_at
// window keeps the meaning (recent health) and lets the partial
// idx_enrichment_queue_error_created serve it.
const extractionHealthWindow = 7 * 24 * time.Hour

// extractionHealth counts enrichment_queue jobs whose latest last_error carries
// each extraction outcome marker (the stable ExtractionReason strings). last_error
// is JSONB on Postgres and text on SQLite; a substring match over its text form
// covers both the structured warnings ({"warnings":[{"reason":...}]}) and the
// top-level failure payloads without parsing per-backend JSON paths. Scoped to
// rows created within extractionHealthWindow (see the const).
func (s *EnrichmentAdminStore) extractionHealth(ctx context.Context) (api.EnrichmentExtractionHealthInfo, error) {
	var info api.EnrichmentExtractionHealthInfo
	errText := "last_error::text"
	placeholder := "$1"
	if s.db.Backend() != storage.BackendPostgres {
		errText = "CAST(last_error AS TEXT)"
		placeholder = "?"
	}
	cnt := func(marker string) string {
		return fmt.Sprintf("COALESCE(SUM(CASE WHEN %s LIKE '%%%s%%' THEN 1 ELSE 0 END),0)", errText, marker)
	}
	q := fmt.Sprintf(`SELECT %s, %s, %s, %s, %s FROM enrichment_queue WHERE last_error IS NOT NULL AND created_at > %s`,
		cnt("partial_recovery"), cnt("parse_failed"), cnt("length_no_recovery"),
		cnt("empty_response"), cnt("llm_call_failed"), placeholder)
	cutoff := time.Now().UTC().Add(-extractionHealthWindow).Format(time.RFC3339)
	row := s.db.QueryRow(ctx, q, cutoff)
	if err := row.Scan(&info.PartialRecovery, &info.ParseFailed, &info.LengthNoRecovery,
		&info.EmptyResponse, &info.LLMCallFailed); err != nil {
		return api.EnrichmentExtractionHealthInfo{}, fmt.Errorf("extraction health counts: %w", err)
	}
	return info, nil
}

// OrgNamespacePath returns the org's root namespace path, the path-prefix scope
// for org-tier per-memory re-extraction. Exported wrapper over orgNamespacePath.
func (s *EnrichmentAdminStore) OrgNamespacePath(ctx context.Context, orgID uuid.UUID) (string, error) {
	return s.orgNamespacePath(ctx, orgID)
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
	// Org owners see project UUIDs only (no names) for projects owned by other
	// users in the org, matching the system-tier privacy posture.
	return s.scopedQueueStatus(ctx, orgPath, orgPath+"/%", false, params)
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

// ClearCompletedJobs deletes completed enrichment_queue rows so the queue view
// stays readable as the table accumulates history. When olderThanDays > 0 only
// rows completed before that cutoff are removed; 0 deletes all completed rows.
// It is scoped strictly to status='completed' and never touches pending or
// processing rows, so it cannot strand an in-flight memory. Returns the number
// of rows deleted.
func (s *EnrichmentAdminStore) ClearCompletedJobs(ctx context.Context, olderThanDays int) (int64, error) {
	pg := s.db.Backend() == storage.BackendPostgres
	var query string
	var args []any
	if olderThanDays > 0 {
		cutoff := time.Now().UTC().AddDate(0, 0, -olderThanDays).Format(time.RFC3339)
		if pg {
			query = `DELETE FROM enrichment_queue WHERE status = 'completed' AND completed_at < $1`
		} else {
			query = `DELETE FROM enrichment_queue WHERE status = 'completed' AND completed_at < ?`
		}
		args = []any{cutoff}
	} else {
		query = `DELETE FROM enrichment_queue WHERE status = 'completed'`
	}
	res, err := s.db.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("clear completed jobs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("clear completed jobs rows affected: %w", err)
	}
	return n, nil
}

// deleteFailedInNamespacePath hard-deletes failed enrichment jobs whose memory
// namespace matches or is descended from prefix. An empty prefix means global
// (no scope filter), the admin path. When olderThanDays > 0 only rows whose
// updated_at (the last-failure timestamp, matching the retention sweep's
// DeleteFailedBefore) is older than the cutoff are removed; 0 deletes every
// failed row in scope. Scoped strictly to status='failed', so it never touches
// pending/processing/completed rows and cannot strand an in-flight memory. It is
// a single set-based DELETE (not the retry path's per-row loop, which on SQLite
// serializes tens of thousands of writes and hangs). Returns rows deleted.
func (s *EnrichmentAdminStore) deleteFailedInNamespacePath(ctx context.Context, prefix string, olderThanDays int) (int64, error) {
	ph := placeholderFn(s.db.Backend() == storage.BackendPostgres)
	query := "DELETE FROM enrichment_queue WHERE status = 'failed'"
	var args []any
	if olderThanDays > 0 {
		cutoff := time.Now().UTC().AddDate(0, 0, -olderThanDays).Format(time.RFC3339)
		query += " AND updated_at < " + ph()
		args = append(args, cutoff)
	}
	if prefix != "" {
		// No table alias: SQLite DELETE does not accept one. namespace_id is the
		// denormalized memory namespace (see scopedNamespaceSubtree).
		query += " AND namespace_id IN (SELECT id FROM namespaces WHERE path = " + ph() + " OR path LIKE " + ph() + ")"
		args = append(args, prefix, prefix+"/%")
	}
	res, err := s.db.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("clear failed jobs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("clear failed jobs rows affected: %w", err)
	}
	return n, nil
}

// SelfClearFailed deletes failed jobs whose memory namespace is descended from
// (or equal to) the caller's user namespace path.
func (s *EnrichmentAdminStore) SelfClearFailed(ctx context.Context, userNamespacePath string, olderThanDays int) (int64, error) {
	return s.deleteFailedInNamespacePath(ctx, userNamespacePath, olderThanDays)
}

// OrgClearFailed deletes failed jobs scoped to the given org's namespace subtree.
func (s *EnrichmentAdminStore) OrgClearFailed(ctx context.Context, orgID uuid.UUID, olderThanDays int) (int64, error) {
	orgPath, err := s.orgNamespacePath(ctx, orgID)
	if err != nil {
		return 0, err
	}
	return s.deleteFailedInNamespacePath(ctx, orgPath, olderThanDays)
}

// ClearFailedJobs deletes failed jobs globally (the system/admin path). Shares
// the one set-based mechanism with the org/self paths via an empty prefix.
func (s *EnrichmentAdminStore) ClearFailedJobs(ctx context.Context, olderThanDays int) (int64, error) {
	return s.deleteFailedInNamespacePath(ctx, "", olderThanDays)
}

func (s *EnrichmentAdminStore) SetPaused(ctx context.Context, paused bool) error {
	value, _ := json.Marshal(paused)
	setting := &model.Setting{
		Key:   service.SettingEnrichmentPaused,
		Value: json.RawMessage(value),
		Scope: "global",
	}
	if err := s.settingsRepo.Set(ctx, setting); err != nil {
		return err
	}
	// The worker run loop and the SSE pool tick read enrichment.paused through
	// the cached SettingsService resolver. Writing via settingsRepo directly
	// (rather than SettingsService.Set) skips that cache's own invalidation, so
	// without this eviction the change would not reach readers until the entry's
	// cache TTL elapsed (~30s) — pause/resume would visibly lag. Mirrors the
	// invalidation SettingsAdminStore.UpdateSetting performs for the same reason.
	if s.settingsSvc != nil {
		s.settingsSvc.InvalidateCache(service.SettingEnrichmentPaused, "global")
	}
	return nil
}

func (s *EnrichmentAdminStore) IsPaused(ctx context.Context) (bool, error) {
	setting, err := s.settingsRepo.Get(ctx, service.SettingEnrichmentPaused, "global")
	if err != nil {
		return false, nil // not set = not paused
	}

	var paused bool
	if err := json.Unmarshal(setting.Value, &paused); err != nil {
		return false, nil
	}
	return paused, nil
}
