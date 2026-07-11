package dreaming

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// rootNamespaceID is the root namespace seeded by migration 000001, the parent
// every test namespace hangs off of.
var rootNamespaceID = uuid.MustParse("00000000-0000-0000-0000-000000000000")

// seedUncoveredMemory creates a namespace, a project, one uncovered memory (no
// enrichment job), and a dream cycle to attach dream_logs to. Returns the memory
// ID and the cycle so the phase can run against real rows with FKs satisfied.
func seedUncoveredMemory(t *testing.T, ctx context.Context, db storage.DB) (uuid.UUID, *model.DreamCycle) {
	t.Helper()
	suffix := uuid.NewString()[:8]

	nsID := uuid.New()
	ns := &model.Namespace{
		ID:       nsID,
		Name:     "NS " + suffix,
		Slug:     "ns-" + suffix,
		Kind:     "org",
		ParentID: &rootNamespaceID,
		Path:     nsID.String(),
		Depth:    1,
	}
	if err := storage.NewNamespaceRepo(db).Create(ctx, ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      nsID,
		OwnerNamespaceID: nsID,
		Name:             "Proj " + suffix,
		Slug:             "proj-" + suffix,
		Settings:         json.RawMessage(`{}`),
	}
	if err := storage.NewProjectRepo(db).Create(ctx, project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	mem := &model.Memory{
		NamespaceID: nsID,
		Content:     "The quick brown fox jumps over the lazy dog.",
		Confidence:  0.95,
		Importance:  0.7,
		Tags:        []string{"test"},
		Metadata:    json.RawMessage(`{}`),
	}
	if err := storage.NewMemoryRepo(db).Create(ctx, mem); err != nil {
		t.Fatalf("create memory: %v", err)
	}

	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: project.ID, NamespaceID: nsID}
	if err := storage.NewDreamCycleRepo(db).Create(ctx, cycle); err != nil {
		t.Fatalf("create dream cycle: %v", err)
	}
	return mem.ID, cycle
}

func countPendingJobs(t *testing.T, ctx context.Context, db storage.DB, memID uuid.UUID) int {
	t.Helper()
	rows, err := db.Query(ctx,
		`SELECT COUNT(*) FROM enrichment_queue WHERE memory_id = ? AND status = 'pending'`,
		memID.String())
	if err != nil {
		t.Fatalf("count pending jobs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan count: %v", err)
		}
	}
	return n
}

// uncoveredSummaryEnqueued returns the "enqueued" count from the persisted
// uncovered_backfill phase_summary dream_logs row, or -1 if no such row exists.
func uncoveredSummaryEnqueued(t *testing.T, ctx context.Context, db storage.DB, cycleID uuid.UUID) int {
	t.Helper()
	rows, err := db.Query(ctx,
		`SELECT after_state FROM dream_logs
		 WHERE cycle_id = ? AND phase = ? AND operation = ?`,
		cycleID.String(), model.DreamPhaseUncoveredBackfill, model.DreamOpPhaseSummary)
	if err != nil {
		t.Fatalf("query dream_logs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return -1
	}
	var afterState string
	if err := rows.Scan(&afterState); err != nil {
		t.Fatalf("scan after_state: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(afterState), &m); err != nil {
		t.Fatalf("unmarshal after_state %q: %v", afterState, err)
	}
	v, ok := m["enqueued"].(float64)
	if !ok {
		t.Fatalf("after_state missing numeric enqueued: %q", afterState)
	}
	return int(v)
}

// TestUncoveredBackfillPhase_Integration_EnqueuesAndLogs runs the real phase
// (real UncoveredBackfiller over real SQL + real DreamLogWriter) against a
// migrated SQLite DB and asserts it enqueues a pending job for the uncovered
// memory AND persists an uncovered_backfill phase_summary row the Dreaming
// Monitor reads.
func TestUncoveredBackfillPhase_Integration_EnqueuesAndLogs(t *testing.T) {
	ctx := context.Background()
	db := newMigratedTestDB(t)
	memID, cycle := seedUncoveredMemory(t, ctx, db)

	logger := NewDreamLogWriter(storage.NewDreamLogRepo(db), cycle.ID, cycle.ProjectID)
	phase := NewUncoveredBackfillPhase(storage.NewUncoveredBackfiller(db), uncoveredSettings(true))

	if _, err := phase.Execute(ctx, cycle, NewTokenBudget(10000, 2048), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got := countPendingJobs(t, ctx, db, memID); got != 1 {
		t.Errorf("pending jobs for uncovered memory = %d, want 1", got)
	}
	if got := uncoveredSummaryEnqueued(t, ctx, db, cycle.ID); got != 1 {
		t.Errorf("persisted uncovered_backfill summary enqueued = %d, want 1 (row present with count 1)", got)
	}
}

// TestUncoveredBackfillPhase_Integration_EnrichedNotReenqueued is the durable-
// coverage regression guard: a memory already stamped enriched = true is NOT
// re-enqueued, even though it has no enrichment_queue row (mirroring a completed
// job whose queue row was cleared by the admin clear-completed endpoint). Proves
// the phase keys on memories.enriched, not on queue-row presence.
func TestUncoveredBackfillPhase_Integration_EnrichedNotReenqueued(t *testing.T) {
	ctx := context.Background()
	db := newMigratedTestDB(t)
	memID, cycle := seedUncoveredMemory(t, ctx, db)

	// Stamp the (only) memory enriched, as finalizeJob would, leaving no queue row.
	if err := storage.NewMemoryRepo(db).MarkEnriched(ctx, memID, cycle.NamespaceID, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("mark enriched: %v", err)
	}

	logger := NewDreamLogWriter(storage.NewDreamLogRepo(db), cycle.ID, cycle.ProjectID)
	phase := NewUncoveredBackfillPhase(storage.NewUncoveredBackfiller(db), uncoveredSettings(true))

	if _, err := phase.Execute(ctx, cycle, NewTokenBudget(10000, 2048), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got := countPendingJobs(t, ctx, db, memID); got != 0 {
		t.Errorf("enriched memory was re-enqueued: %d pending jobs, want 0", got)
	}
	// The phase still runs (enrichment enabled) and writes a summary, but with a
	// zero enqueue count since nothing is uncovered.
	if got := uncoveredSummaryEnqueued(t, ctx, db, cycle.ID); got != 0 {
		t.Errorf("persisted uncovered_backfill summary enqueued = %d, want 0", got)
	}
}

// TestUncoveredBackfillPhase_Integration_DisabledEnqueuesNothing proves the
// runtime gate: with enrichment.enabled false, a real cycle enqueues nothing and
// writes no uncovered_backfill dream_logs row.
func TestUncoveredBackfillPhase_Integration_DisabledEnqueuesNothing(t *testing.T) {
	ctx := context.Background()
	db := newMigratedTestDB(t)
	memID, cycle := seedUncoveredMemory(t, ctx, db)

	logger := NewDreamLogWriter(storage.NewDreamLogRepo(db), cycle.ID, cycle.ProjectID)
	phase := NewUncoveredBackfillPhase(storage.NewUncoveredBackfiller(db), uncoveredSettings(false))

	if _, err := phase.Execute(ctx, cycle, NewTokenBudget(10000, 2048), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got := countPendingJobs(t, ctx, db, memID); got != 0 {
		t.Errorf("enrichment-disabled cycle enqueued %d jobs, want 0", got)
	}
	if got := uncoveredSummaryEnqueued(t, ctx, db, cycle.ID); got != -1 {
		t.Errorf("enrichment-disabled cycle wrote a dream_logs summary (enqueued=%d), want none", got)
	}
}
